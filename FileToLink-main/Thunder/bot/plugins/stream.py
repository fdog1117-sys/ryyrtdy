import os
import json
import asyncio
import secrets
from typing import Any, Dict, Optional

from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait, MessageNotModified, MessageDeleteForbidden, MessageIdInvalid
from pyrogram.types import (InlineKeyboardButton, InlineKeyboardMarkup, Message)
from huggingface_hub import HfApi

from Thunder.bot import StreamBot
from Thunder.utils.bot_utils import (gen_canonical_links, gen_links, is_admin,
                                     log_newusr, notify_own, reply_user_err)
from Thunder.utils.canonical_files import get_or_create_canonical_file
from Thunder.utils.database import db
from Thunder.utils.decorators import (check_banned, get_shortener_status, require_token)
from Thunder.utils.force_channel import force_channel_check
from Thunder.utils.logger import logger
from Thunder.utils.human_readable import humanbytes
from Thunder.utils.file_properties import get_media, get_fname, get_fsize
from Thunder.utils.messages import (
    MSG_BATCH_LINKS_READY, MSG_BUTTON_DOWNLOAD, MSG_BUTTON_START_CHAT,
    MSG_BUTTON_STREAM_NOW, MSG_CRITICAL_ERROR, MSG_DM_BATCH_PREFIX,
    MSG_DM_SINGLE_PREFIX, MSG_ERROR_DM_FAILED, MSG_ERROR_INVALID_NUMBER,
    MSG_ERROR_NO_FILE, MSG_ERROR_NOT_ADMIN, MSG_ERROR_NUMBER_RANGE,
    MSG_ERROR_PROCESSING_MEDIA, MSG_ERROR_REPLY_FILE, MSG_ERROR_START_BOT,
    MSG_LINKS, MSG_NEW_FILE_REQUEST, MSG_PROCESSING_BATCH,
    MSG_PROCESSING_FILE, MSG_PROCESSING_REQUEST, MSG_PROCESSING_RESULT,
    MSG_PROCESSING_STATUS
)
from Thunder.utils.rate_limiter import handle_rate_limited_request
from Thunder.vars import Var

BATCH_SIZE = 10
LINK_CHUNK_SIZE = 20
BATCH_UPDATE_INTERVAL = 5
MESSAGE_DELAY = 0.5

# 🌐 读取 HF 存储桶变量与本地持久化索引
HF_TOKEN = os.environ.get("HF_TOKEN")
DATASET_REPO = os.environ.get("DATASET_REPO")
METADATA_FILE = "metadata_db.json"

if not os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, "w") as f:
        json.dump({}, f)

def save_meta(msg_id, data):
    with open(METADATA_FILE, "r+") as f:
        db = json.load(f)
        db[str(msg_id)] = data
        f.seek(0); json.dump(db, f, indent=4); f.truncate()


async def fwd_media(m_msg: Message) -> Optional[Message]:
    try:
        try:
            return await m_msg.copy(chat_id=Var.BIN_CHANNEL)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await m_msg.copy(chat_id=Var.BIN_CHANNEL)
    except Exception as e:
        if "MEDIA_CAPTION_TOO_LONG" in str(e):
            logger.debug(f"MEDIA_CAPTION_TOO_LONG error, retrying without caption: {e}")
            try:
                return await m_msg.copy(chat_id=Var.BIN_CHANNEL, caption=None)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                return await m_msg.copy(chat_id=Var.BIN_CHANNEL, caption=None)
        logger.error(f"Error fwd_media copy: {e}", exc_info=True)
        return None


def get_link_buttons(links):
    # 重写按钮：由于存进了存储桶，直接让 Download 和 Stream 按钮都指向永久 CDN 直链
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(MSG_BUTTON_STREAM_NOW, url=links['stream_link']),
        InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=links['online_link'])
    ]])

async def validate_request_common(client: Client, message: Message) -> Optional[bool]:
    if not await check_banned(client, message):
        return None
    if not await require_token(client, message):
        return None
    if not await force_channel_check(client, message):
        return None
    return await get_shortener_status(client, message)


async def send_channel_links(
    links: Dict[str, Any],
    source_info: str,
    source_id: int,
    *,
    target_msg: Optional[Message] = None,
    reply_to_message_id: Optional[int] = None
):
    try:
        text = MSG_NEW_FILE_REQUEST.format(
            source_info=source_info,
            id_=source_id,
            online_link=links['online_link'],
            stream_link=links['stream_link']
        )
        if target_msg:
            await target_msg.reply_text(text, disable_web_page_preview=True, quote=True)
        else:
            await StreamBot.send_message(chat_id=Var.BIN_CHANNEL, text=text, disable_web_page_preview=True, reply_to_message_id=reply_to_message_id)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        text = MSG_NEW_FILE_REQUEST.format(
            source_info=source_info,
            id_=source_id,
            online_link=links['online_link'],
            stream_link=links['stream_link']
        )
        if target_msg:
            await target_msg.reply_text(text, disable_web_page_preview=True, quote=True)
        else:
            await StreamBot.send_message(chat_id=Var.BIN_CHANNEL, text=text, disable_web_page_preview=True, reply_to_message_id=reply_to_message_id)


async def safe_edit_message(message: Message, text: str, **kwargs):
    try:
        try:
            return await message.edit_text(text, **kwargs)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await message.edit_text(text, **kwargs)
    except MessageNotModified:
        pass
    except MessageDeleteForbidden:
        logger.debug(f"Failed to edit message {message.id} due to permissions.")
    except Exception as e:
        logger.error(f"Error editing message {message.id}: {e}", exc_info=True)


async def safe_delete_message(message: Message):
    try:
        try:
            await message.delete()
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.delete()
    except MessageDeleteForbidden:
        logger.debug(f"Failed to delete message {message.id} due to permissions.")
    except Exception as e:
        logger.error(f"Error deleting message {message.id}: {e}", exc_info=True)


async def send_dm_links(bot: Client, user_id: int, links: Dict[str, Any], chat_title: str):
    try:
        dm_text = MSG_DM_SINGLE_PREFIX.format(chat_title=chat_title) + "\n" + \
                  MSG_LINKS.format(
                      file_name=links['media_name'],
                      file_size=links['media_size'],
                      download_link=links['online_link'],
                      stream_link=links['stream_link']
                  )
        try:
            await bot.send_message(chat_id=user_id, text=dm_text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=get_link_buttons(links))
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await bot.send_message(chat_id=user_id, text=dm_text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=get_link_buttons(links))
    except Exception as e:
        logger.error(f"Error sending DM to user {user_id}: {e}", exc_info=True)


async def send_link(msg: Message, links: Dict[str, Any]):
    try:
        await msg.reply_text(
            MSG_LINKS.format(
                file_name=links['media_name'],
                file_size=links['media_size'],
                download_link=links['online_link'],
                stream_link=links['stream_link']
            ),
            quote=True,
            parse_mode=enums.ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=get_link_buttons(links)
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await msg.reply_text(
            MSG_LINKS.format(
                file_name=links['media_name'],
                file_size=links['media_size'],
                download_link=links['online_link'],
                stream_link=links['stream_link']
            ),
            quote=True,
            parse_mode=enums.ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=get_link_buttons(links)
        )


@StreamBot.on_message(filters.command("link") & ~filters.private)
async def link_handler(bot: Client, msg: Message, **kwargs):
    async def _actual_link_handler(client: Client, message: Message, **handler_kwargs):
        shortener_val = await validate_request_common(client, message)
        if shortener_val is None:
            return
        if message.from_user and not await db.is_user_exist(message.from_user.id):
            invite_link = f"https://t.me/{client.me.username}?start=start"
            try:
                await message.reply_text(MSG_ERROR_START_BOT.format(invite_link=invite_link), disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(MSG_BUTTON_START_CHAT, url=invite_link)]]), quote=True)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await message.reply_text(MSG_ERROR_START_BOT.format(invite_link=invite_link), disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(MSG_BUTTON_START_CHAT, url=invite_link)]]), quote=True)
            return

        if (message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP] and not await is_admin(client, message.chat.id)):
            await reply_user_err(message, MSG_ERROR_NOT_ADMIN)
            return

        if not message.reply_to_message or not message.reply_to_message.media:
            await reply_user_err(message, MSG_ERROR_REPLY_FILE if not message.reply_to_message else MSG_ERROR_NO_FILE)
            return

        notification_msg = handler_kwargs.get('notification_msg')
        parts = message.text.split()
        num_files = 1
        if len(parts) > 1:
            try:
                num_files = int(parts[1])
                if not 1 <= num_files <= Var.MAX_BATCH_FILES:
                    await reply_user_err(message, MSG_ERROR_NUMBER_RANGE.format(max_files=Var.MAX_BATCH_FILES))
                    return
            except ValueError:
                await reply_user_err(message, MSG_ERROR_INVALID_NUMBER)
                return

        try:
            status_msg = await message.reply_text(MSG_PROCESSING_REQUEST, quote=True)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            status_msg = await message.reply_text(MSG_PROCESSING_REQUEST, quote=True)
        shortener_val = handler_kwargs.get('shortener', shortener_val)
        if num_files == 1:
            await process_single(client, message, message.reply_to_message, status_msg, shortener_val, notification_msg=notification_msg)
        else:
            await process_batch(client, message, message.reply_to_message.id, num_files, status_msg, shortener_val, notification_msg=notification_msg)

    await handle_rate_limited_request(bot, msg, _actual_link_handler, **kwargs)


@StreamBot.on_message(filters.private & filters.incoming & (filters.document | filters.video | filters.photo | filters.audio | filters.voice | filters.animation | filters.video_note), group=4)
async def private_receive_handler(bot: Client, msg: Message, **kwargs):
    async def _actual_private_receive_handler(client: Client, message: Message, **handler_kwargs):
        shortener_val = await validate_request_common(client, message)
        if shortener_val is None:
            return
        if not message.from_user:
            return

        notification_msg = handler_kwargs.get('notification_msg')
        await log_newusr(client, message.from_user.id, message.from_user.first_name or "")
        try:
            status_msg = await message.reply_text(MSG_PROCESSING_FILE, quote=True)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            status_msg = await message.reply_text(MSG_PROCESSING_FILE, quote=True)
        await process_single(client, message, message, status_msg, shortener_val, notification_msg=notification_msg)

    await handle_rate_limited_request(bot, msg, _actual_private_receive_handler, **kwargs)


@StreamBot.on_message(filters.channel & filters.incoming & (filters.document | filters.video | filters.audio) & ~filters.chat(Var.BIN_CHANNEL), group=-1)
async def channel_receive_handler(bot: Client, msg: Message):
    async def _actual_channel_receive_handler(client: Client, message: Message, **handler_kwargs):
        if not Var.CHANNEL:
            return
        notification_msg = handler_kwargs.get('notification_msg')
        is_banned_statically = hasattr(Var, 'BANNED_CHANNELS') and message.chat.id in Var.BANNED_CHANNELS
        is_banned_dynamically = await db.is_channel_banned(message.chat.id) is not None

        if is_banned_statically or is_banned_dynamically:
            try:
                try:
                    await client.leave_chat(message.chat.id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await client.leave_chat(message.chat.id)
            except Exception as e:
                logger.error(f"Error leaving banned channel {message.chat.id}: {e}")
            return
        if not await is_admin(client, message.chat.id):
            logger.debug(f"Bot is not admin in channel {message.chat.id} ({message.chat.title or 'Unknown'}). Ignoring message.")
            return

        try:
            shortener_val = await get_shortener_status(client, message)
            # 在群组和频道处理中，我们同样调用魔改后的 process_single
            await process_single(client, message, message, None, shortener_val, notification_msg=notification_msg)
        except Exception as e:
            logger.error(f"Error in _actual_channel_receive_handler for message {message.id}: {e}", exc_info=True)

    rl_user_id = None
    if msg.sender_chat and msg.sender_chat.id:
        rl_user_id = msg.sender_chat.id
    elif msg.from_user:
        rl_user_id = msg.from_user.id
    
    if rl_user_id is None:
        await _actual_channel_receive_handler(bot, msg)
        return

    await handle_rate_limited_request(bot, msg, _actual_channel_receive_handler, rl_user_id=rl_user_id)


# 👑 【微创手术拦截核心】：彻底接管单文件处理终点站
async def process_single(
    bot: Client,
    msg: Message,
    file_msg: Message,
    status_msg: Optional[Message],
    shortener_val: bool,
    original_request_msg: Optional[Message] = None,
    notification_msg: Optional[Message] = None
):
    try:
        # 如果调用时没传 status_msg（比如批量模式中），我们现场补一个提示
        if not status_msg:
            try:
                status_msg = await msg.reply_text("⏳ 正在拉取文件并同步至存储桶...", quote=True)
            except Exception:
                pass

        media = get_media(file_msg)
        if not media:
            if status_msg: await safe_edit_message(status_msg, "⚠️ 未找到可下载的媒体文件。")
            return None

        file_name = get_fname(file_msg)
        file_size = get_fsize(file_msg)

        # 1. 拦截流式输出，启动内网带宽，完整下载文件到本地容器
        local_path = await file_msg.download(file_name=file_name)
        if not local_path or not os.path.exists(local_path):
            raise Exception("文件本地下载到容器盘失败。")

        if not HF_TOKEN or not DATASET_REPO:
            raise Exception("未检测到环境变量中的 HF_TOKEN 或 DATASET_REPO 配置！")

        # 2. 多线程无阻塞推送到你指定的 HF Dataset 存储桶中
        hf_api = HfApi(token=HF_TOKEN)
        storage_path = f"files/{file_msg.id}_{file_name}"
        
        await asyncio.to_thread(
            hf_api.upload_file,
            path_or_fileobj=local_path,
            path_in_repo=storage_path,
            repo_id=DATASET_REPO,
            repo_type="dataset"
        )

        # 3. 推送完成后立斩本地副本，防止撑爆容器盘
        if os.path.exists(local_path):
            os.remove(local_path)

        # 4. 获取永久 CDN 直链，并录入元数据持久化本地 DB 索引
        cdn_url = f"https://huggingface.co/datasets/{DATASET_REPO}/resolve/main/{storage_path}"
        meta_data = {
            "file_name": file_name,
            "file_size": file_size,
            "mime_type": getattr(media, "mime_type", "application/octet-stream"),
            "tg_message_id": file_msg.id,
            "cdn_url": cdn_url
        }
        save_meta(file_msg.id, meta_data)

        # 5. 伪装原本 Thunder 的 links 字典，无缝向下游组件投递，确保完美的排版按钮不受影响
        links = {
            "media_name": file_name,
            "media_size": humanbytes(file_size),
            "online_link": cdn_url,
            "stream_link": cdn_url
        }

        # 6. 恢复原版的标准输出流界面
        if notification_msg:
            await safe_edit_message(
                notification_msg,
                MSG_LINKS.format(file_name=links['media_name'], file_size=links['media_size'], download_link=links['online_link'], stream_link=links['stream_link']),
                parse_mode=enums.ParseMode.MARKDOWN,
                disable_web_page_preview=True,
                reply_markup=get_link_buttons(links)
            )
        elif not original_request_msg:
            await send_link(msg, links)

        if msg.chat.type != enums.ChatType.PRIVATE and msg.from_user and not original_request_msg:
            await send_dm_links(bot, msg.from_user.id, links, msg.chat.title or "the chat")

        if status_msg:
            await safe_delete_message(status_msg)
            
        return links

    except Exception as e:
        logger.error(f"Error processing single file for message {file_msg.id}: {e}", exc_info=True)
        if status_msg:
            await safe_edit_message(status_msg, f"❌ 同步存储桶失败：{str(e)}")
        
        await notify_own(bot, MSG_CRITICAL_ERROR.format(error=str(e), error_id=secrets.token_hex(6)))
        return None


async def process_batch(
    bot: Client,
    msg: Message,
    start_id: int,
    count: int,
    status_msg: Message,
    shortener_val: bool,
    notification_msg: Optional[Message] = None
):
    processed = 0
    failed = 0
    links_list = []
    for batch_start in range(0, count, BATCH_SIZE):
        batch_size = min(BATCH_SIZE, count - batch_start)
        batch_ids = list(range(start_id + batch_start, start_id + batch_start + batch_size))
        try:
            try:
                await status_msg.edit_text(MSG_PROCESSING_BATCH.format(batch_number=(batch_start // BATCH_SIZE) + 1, total_batches=(count + BATCH_SIZE - 1) // BATCH_SIZE, file_count=batch_size))
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await status_msg.edit_text(MSG_PROCESSING_BATCH.format(batch_number=(batch_start // BATCH_SIZE) + 1, total_batches=(count + BATCH_SIZE - 1) // BATCH_SIZE, file_count=batch_size))
        except MessageNotModified:
            pass
        try:
            try:
                messages = await bot.get_messages(msg.chat.id, batch_ids)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                messages = await bot.get_messages(msg.chat.id, batch_ids)
            if messages is None:
                messages = []
        except Exception as e:
            logger.error(f"Error getting messages in batch: {e}", exc_info=True)
            messages = []
        for m in messages:
            if m and m.media:
                links = await process_single(bot, msg, m, None, shortener_val, original_request_msg=msg)
                if links:
                    links_list.append(links['online_link'])
                    processed += 1
                else:
                    failed += 1
            else:
                failed += 1
        if (processed + failed) % BATCH_UPDATE_INTERVAL == 0 or (processed + failed) == count:
            try:
                try:
                    await status_msg.edit_text(MSG_PROCESSING_STATUS.format(processed=processed, total=count, failed=failed))
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await status_msg.edit_text(MSG_PROCESSING_STATUS.format(processed=processed, total=count, failed=failed))
            except MessageNotModified:
                pass
    for i in range(0, len(links_list), LINK_CHUNK_SIZE):
        chunk = links_list[i:i+LINK_CHUNK_SIZE]
        chunk_text = MSG_BATCH_LINKS_READY.format(count=len(chunk)) + f"\n\n<code>{chr(10).join(chunk)}</code>"
        try:
            await msg.reply_text(chunk_text, quote=True, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await msg.reply_text(chunk_text, quote=True, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
        if msg.chat.type != enums.ChatType.PRIVATE and msg.from_user:
            try:
                try:
                    await bot.send_message(chat_id=msg.from_user.id, text=MSG_DM_BATCH_PREFIX.format(chat_title=msg.chat.title or "the chat") + "\n" + chunk_text, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await bot.send_message(chat_id=msg.from_user.id, text=MSG_DM_BATCH_PREFIX.format(chat_title=msg.chat.title or "the chat") + "\n" + chunk_text, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error sending DM in batch: {e}", exc_info=True)
                await reply_user_err(msg, MSG_ERROR_DM_FAILED)
        if i + LINK_CHUNK_SIZE < len(links_list):
            await asyncio.sleep(MESSAGE_DELAY)
    try:
        await status_msg.edit_text(MSG_PROCESSING_RESULT.format(processed=processed, total=count, failed=failed))
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await status_msg.edit_text(MSG_PROCESSING_RESULT.format(processed=processed, total=count, failed=failed))
    if notification_msg:
        await safe_delete_message(notification_msg)