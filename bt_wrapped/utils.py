from pyrogram import Client
from typing import Union
from key.key import API


def get_engagement_from_chat_id(
    chat_id: Union[str, int], message_id, pyro: Client, control: bool = False
):
    """
    returns the engagemente as a dict.
    """
    message = pyro.get_messages(chat_id, message_id)
    if control:
        print(message)
    result = dict()
    result["views"] = message.views
    result["forwards"] = message.forwards
    reacts = dict()
    try:
        for i in message.reactions.reactions:
            reacts[i.emoji] = i.count
        print(message_id)
    except Exception as e:
        print(e)
    result["reactions"] = reacts
    return result


if __name__ == "__main__":
    with Client("pyro_session", API.app_id, API.api_hash) as client:
        print(get_engagement_from_chat_id(-1001155308424, 446309, client, True))
    # https://t.me/c/1155308424/629700
