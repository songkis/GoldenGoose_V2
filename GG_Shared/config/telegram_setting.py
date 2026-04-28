import logging

logger = logging.getLogger(__name__)

TOKEN = ""
CHAT_ID = ""


def setTelegramConfChatId(chatId):
    """텔레그램 Chat ID DB 저장"""
    from SQL.sql import REPLACE_TB_AI_CONF
    from util.CommUtils import get_db_connection

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                REPLACE_TB_AI_CONF,
                ("11", "TELEGRAM_CHAT_ID", chatId, "텔레그램 발급 chatID"),
            )
    except Exception as e:
        if "logger" in globals():
            logger.error(f"setTelegramConfChatId Error: {e}")
    finally:
        if "cursor" in locals():
            cursor.close()


def ToTelegram(message_str):
    """Telegram 메시지 전송 (분할 전송 대응)"""
    from util.CommUtils import getAIConfVal

    global TOKEN, CHAT_ID
    if not message_str:
        return
    if TOKEN == "":
        TOKEN = getAIConfVal("TELEGRAM_TOKEN")
    if not TOKEN:
        return

    import asyncio
    import telegram

    try:
        bot = telegram.Bot(token=TOKEN)
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

        if CHAT_ID == "":
            CHAT_ID = getAIConfVal("TELEGRAM_CHAT_ID")
        if CHAT_ID == "":
            updates = asyncio.run(botgetUpdates(bot))
            last_message = next((u for u in reversed(updates) if u is not None), None)
            if last_message:
                CHAT_ID = last_message.message.chat.id
                setTelegramConfChatId(CHAT_ID)

        if CHAT_ID:
            MAX_LENGTH = 4000
            for i in range(0, len(message_str), MAX_LENGTH):
                chunk = message_str[i : i + MAX_LENGTH]
                try:
                    asyncio.run(sendMsgTelegram(bot, chunk))
                except Exception as ce:
                    if "logger" in globals():
                        logger.error(f"Telegram Chunk Error: {ce}")
    except Exception as e:
        if "logger" in globals():
            logger.error(f"ToTelegram Error: {e}")


async def sendMsgTelegram(bot, str):
    from config.ai_settings import 거래환경

    await bot.sendMessage(chat_id=CHAT_ID, text=f"[{거래환경}] " + str)


async def botgetUpdates(bot):
    return await bot.getUpdates()
