import argparse
import asyncio
import json
import os
import platform
import traceback

import httpx
import aiofiles
import tqdm

import httpx
from CanvasSyncer import CanvasSyncer

__version__ = "2.0.12"


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".canvassyncer.json"
)

def initConfig():
    oldConfig = {}
    if os.path.exists(CONFIG_PATH):
        oldConfig = json.load(open(CONFIG_PATH))
    elif os.path.exists("./canvassyncer.json"):
        oldConfig = json.load(open("./canvassyncer.json"))

    def promptConfigStr(promptStr, key, *, defaultValOnMissing=None):
        defaultVal = oldConfig.get(key)
        if defaultVal is None:
            if defaultValOnMissing is not None:
                defaultVal = defaultValOnMissing
            else:
                defaultVal = ""
        elif isinstance(defaultVal, list):
            defaultVal = " ".join((str(val) for val in defaultVal))
        defaultVal = str(defaultVal)
        if defaultValOnMissing is not None:
            defaultValOnRemove = defaultValOnMissing
        else:
            defaultValOnRemove = ""
        tipStr = f"(Default: {defaultVal})" if defaultVal else ""
        tipRemove = "(If you input remove, value will become " + (
            f"{defaultValOnRemove})" if defaultValOnRemove != "" else "empty)"
        )
        res = input(f"{promptStr}{tipStr}{tipRemove}: ").strip()
        if not res:
            res = defaultVal
        elif res == "remove":
            res = defaultValOnRemove
        return res

    print("Generating new config file...")
    url = promptConfigStr(
        "Canvas url", "canvasURL", defaultValOnMissing="https://jicanvas.com"
    )
    token = promptConfigStr("Canvas access token", "token")
    courseCodesStr = promptConfigStr(
        "Courses to sync in course codes(split with space)", "courseCodes"
    )
    courseCodes = courseCodesStr.split()
    courseIDsStr = promptConfigStr(
        "Courses to sync in course ID(split with space)", "courseIDs"
    )
    courseIDs = [int(courseID) for courseID in courseIDsStr.split()]
    downloadDir = promptConfigStr(
        "Path to save canvas files",
        "downloadDir",
        defaultValOnMissing=os.path.abspath(""),
    )
    filesizeThreshStr = promptConfigStr(
        "Maximum file size to download(MB)", "filesizeThresh", defaultValOnMissing=250
    )
    allowAudio = promptConfigStr(
        "Whether allow downloading audios", "allowAudio", defaultValOnMissing=True
    )
    allowVideo = promptConfigStr(
        "Whether allow downloading videos", "allowVideo", defaultValOnMissing=True
    )
    allowImage = promptConfigStr(
        "Whether allow downloading images", "allowImage", defaultValOnMissing=True
    )

    try:
        filesizeThresh = float(filesizeThreshStr)
    except Exception:
        filesizeThresh = 250
    allowAudio = (allowAudio == "True") or (allowAudio == "true")
    allowVideo = (allowVideo == "True") or (allowVideo == "true")
    allowImage = (allowImage == "True") or (allowImage == "true")
    return {
        "canvasURL": url,
        "token": token,
        "courseCodes": courseCodes,
        "courseIDs": courseIDs,
        "downloadDir": downloadDir,
        "filesizeThresh": filesizeThresh,
        "allowAudio": allowAudio,
        "allowVideo": allowVideo,
        "allowImage": allowImage,
    }


def getConfig():
    parser = argparse.ArgumentParser(description="A Simple Canvas File Syncer")
    parser.add_argument("-r", help="recreate config file", action="store_true")
    parser.add_argument("-y", help="confirm all prompts", action="store_true")
    parser.add_argument(
        "--no-subfolder",
        help="do not create a course code named subfolder when synchronizing files",
        action="store_true",
    )
    parser.add_argument(
        "-p", "--path", help="appoint config file path", default=CONFIG_PATH
    )
    parser.add_argument(
        "-c",
        "--connection",
        help="max connection count with server",
        default=8,
        type=int,
    )
    parser.add_argument("-x", "--proxy", help="download proxy", default=None)
    parser.add_argument("-V", "--version", action="version", version=__version__)
    parser.add_argument(
        "-d", "--debug", help="show debug information", action="store_true"
    )
    parser.add_argument(
        "--no-keep-older-version",
        help="do not keep older version",
        action="store_true",
    )
    args = parser.parse_args()
    configPath = args.path
    if args.r or not os.path.exists(configPath):
        if not os.path.exists(configPath):
            print("Config file not exist, creating...")
        try:
            json.dump(
                initConfig(),
                open(configPath, mode="w", encoding="utf-8"),
                indent=4,
            )
        except Exception as e:
            print(f"\nError: {e.__class__.__name__}. Failed to create config file.")
            if args.debug:
                print(traceback.format_exc())
            exit(1)
    config = json.load(open(configPath, mode="r", encoding="utf-8"))
    config["y"] = args.y
    config["proxy"] = args.proxy
    config["no_subfolder"] = args.no_subfolder
    config["connection_count"] = args.connection
    config["debug"] = args.debug
    if not "allowAudio" in config:
        config["allowAudio"] = True
    if not "allowVideo" in config:
        config["allowVideo"] = True
    if not "allowImage" in config:
        config["allowImage"] = True
    if not "keepOlderVersion" in config:
        config["no_keep_older_version"] = args.no_keep_older_version

    return config


async def sync():
    syncer = None
    try:
        config = getConfig()
        while True:
            try:
                syncer = CanvasSyncer(config)
                await syncer.sync()
                break
            except httpx.ConnectError as e:
                if config["connection_count"] == 2:
                    raise e
                config["connection_count"] //= 2
                print(
                    "Server connect error, reducing connection count "
                    f"to {config['connection_count']} and retrying..."
                )
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        errorName = e.__class__.__name__
        print(
            f"Unexpected error: {errorName}. Please check your network and token!"
            + ("" if config["debug"] else " Or use -d for detailed information.")
        )
        if config["debug"]:
            print(traceback.format_exc())
    finally:
        if syncer:
            await syncer.aclose()


def run():
    try:
        if platform.system() == "Windows":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(sync())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user, exiting...")
        exit(1)


if __name__ == "__main__":
    run()
