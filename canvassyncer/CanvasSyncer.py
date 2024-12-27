import asyncio
import mimetypes
import os
import re
from datetime import datetime, timezone

from AsyncSemClient import AsyncSemClient


PAGES_PER_TIME = 8


class CanvasSyncer:
    def __init__(self, config):
        self.config = config
        self.client = AsyncSemClient(
            config["connection_count"], config["token"], config.get("proxy")
        )
        self.courseCode = {}
        self.baseUrl = self.config["canvasURL"] + "/api/v1"
        self.downloadDir = self.config["downloadDir"]

        self.folders = {}

        # FILE DICTIONARY: 2-stage dict
        # {courseID:{relative_file_path (to course root):{"name":<str> (localFiles Only), "url":<str> (not for localFiles), size:<int>, modified_date:<int>}}}
        self.localFiles = {}
        self.onlineFiles = {}
        self.laterFiles = {}
        self.newFiles = {}
        self.overSizedFiles = {}
        self.downloadList = {}
        self.downloadSize = 0
        self.totalFileCount = 0
        if not os.path.exists(self.downloadDir):
            os.mkdir(self.downloadDir)

    async def aclose(self):
        await self.client.aclose()

    async def dictFromPages(self, helperFunc, *args, **kwargs):
        res = {}
        page = 1
        endOfPage = False
        while not endOfPage:
            pageRes = await asyncio.gather(
                *[helperFunc(page + i, *args, **kwargs) for i in range(PAGES_PER_TIME)]
            )
            for item in pageRes:
                if not item:
                    endOfPage = True
                res.update(item)
            page += PAGES_PER_TIME
        return res

    def scanLocalFiles(self, courseID, folders):
        localFiles = {}
        for folder in folders.values():
            if self.config["no_subfolder"]:
                path = os.path.join(self.downloadDir, folder[1:])
            else:
                path = os.path.join(
                    self.downloadDir, f"{self.courseCode[courseID]}{folder}"
                )
            if not os.path.exists(path):
                os.makedirs(path)

            for f in os.listdir(path):
                if not os.path.isdir(os.path.join(path, f)):
                    full_file_path = (
                        os.path.join(path, f).replace("\\", "/").replace("//", "/")
                    )
                    relative_file_path = (
                        os.path.join(folder, f).replace("\\", "/").replace("//", "/")
                    )
                    localFiles[relative_file_path] = {
                        "name": f,
                        "size": os.path.getsize(full_file_path),
                        "modified_time": int(os.path.getctime(full_file_path)),
                    }
        return localFiles

    async def getCourseFoldersWithIDHelper(self, page, courseID):
        res = {}
        url = f"{self.baseUrl}/courses/{courseID}/folders?page={page}"
        retryTimes = 0
        while retryTimes < 5:
            try:
                folders = await self.client.json(url, debug=self.config["debug"])
                for folder in folders:
                    if folder["full_name"].startswith("course files"):
                        folder["full_name"] = folder["full_name"][len("course files") :]
                    res[folder["id"]] = folder["full_name"]
                    if not res[folder["id"]]:
                        res[folder["id"]] = "/"
                    res[folder["id"]] = re.sub(
                        r"[\\\:\*\?\"\<\>\|]", "_", res[folder["id"]]
                    )
                return res
            except Exception as e:
                retryTimes = retryTimes + 1
                if self.config["debug"]:
                    print(str(retryTimes) + " time(s) error: " + str(e))

    async def getCourseFilesHelper(self, page, courseID, folders):
        files = {}
        url = f"{self.baseUrl}/courses/{courseID}/files?page={page}"
        canvasFiles = await self.client.json(url, debug=self.config["debug"])
        if not canvasFiles or isinstance(canvasFiles, dict):
            return files
        for f in canvasFiles:
            if f["folder_id"] not in folders.keys():
                continue
            f["display_name"] = re.sub(r"[\/\\\:\*\?\"\<\>\|]", "_", f["display_name"])
            path = f"{folders[f['folder_id']]}/{f['display_name']}"
            path = path.replace("\\", "/").replace("//", "/")
            dt = datetime.strptime(f["modified_at"], "%Y-%m-%dT%H:%M:%SZ")
            modifiedTimeStamp = int(dt.replace(tzinfo=timezone.utc).timestamp())
            response = await self.client.head(f["url"])
            file_size = int(response.get("content-length", 0))
            files[path] = {
                "url": f["url"],
                "modified_time": int(modifiedTimeStamp),
                "size": file_size,
            }
        return files

    async def getCourseFiles(self, courseID):
        self.folders[courseID] = await self.dictFromPages(
            self.getCourseFoldersWithIDHelper, courseID
        )
        self.onlineFiles[courseID] = await self.dictFromPages(
            self.getCourseFilesHelper, courseID, self.folders[courseID]
        )

    async def getCourseIdByCourseCodeHelper(self, page, lowerCourseCodes):
        res = {}
        url = f"{self.baseUrl}/courses?page={page}"
        courses = await self.client.json(
            url, checkError=True, debug=self.config["debug"]
        )
        if not courses:
            return res
        for course in courses:
            if course.get("course_code", "").lower() in lowerCourseCodes:
                res[course["id"]] = course["course_code"]
                lowerCourseCodes.remove(course.get("course_code", "").lower())
                print(
                    f"\t Get course ID: {course['id']} from course code: {course['course_code']}"
                )
            # else:
            # print(f"\t Discard course code: {course['course_code']}")
        return res

    async def getCourseIdByCourseCode(self):
        lowerCourseCodes = []
        for courseCode in self.config["courseCodes"]:
            lowerCourseCodes.append(courseCode.replace(" ", "").lower())
        self.courseCode.update(
            await self.dictFromPages(
                self.getCourseIdByCourseCodeHelper, lowerCourseCodes
            )
        )

    async def getCourseCodeByCourseIDHelper(self, courseID):
        url = f"{self.baseUrl}/courses/{courseID}"
        clientRes = await self.client.json(url, debug=self.config["debug"])
        if clientRes.get("course_code") is None:
            print(f"\t Cannot get course code from course ID: {courseID}")
            return
        self.courseCode[int(courseID)] = clientRes["course_code"]
        print(
            f"\t Get course code: {clientRes['course_code']} from course ID: {courseID}"
        )

    async def getCourseCodeByCourseID(self):
        await asyncio.gather(
            *[
                asyncio.create_task(self.getCourseCodeByCourseIDHelper(courseID))
                for courseID in self.config["courseIDs"]
            ]
        )

    async def getCourseID(self):
        coros = []
        if self.config.get("courseCodes"):
            coros.append(self.getCourseIdByCourseCode())
        if self.config.get("courseIDs"):
            coros.append(self.getCourseCodeByCourseID())
        await asyncio.gather(*coros)

    def checkNewFiles(self):
        if self.skipFiles:
            print(
                "These file(s) will not be synced due to their size"
                + f" (over {self.config['filesizeThresh']} MB):"
            )
            for f in self.skipFiles:
                print(f)
        if self.newFiles:
            print(f"Start to download {len(self.newInfo)} file(s)!")
            for s in self.newInfo:
                print(s)

    def checkLaterFiles(self):
        for courseID in self.laterFiles.keys():
            if self.laterFiles[courseID]:
                break
            else:
                return
        # if not self.laterFiles:
        #     return
        print("\nThese file(s) have later version on canvas:")
        for courseID in self.laterFiles.keys():
            for fileName, file_info in self.laterFiles[courseID].items():
                print(
                    f"\t{self.courseCode[courseID]}{fileName} (Modified at: {datetime.fromtimestamp(file_info['modified_time']).strftime('%Y-%m-%d %H:%M:%S')})"
                )
        isDownload = "Y" if self.config["y"] else input("Update all?(y/n) ")
        while isDownload.lower() not in ["y", "n"]:
            isDownload = input("Please input 'y' or 'n': ")
        if isDownload.lower() == "n":
            return
        # Remove local files with older version
        for courseID in self.laterFiles.keys():
            for fileName, file_info in self.laterFiles[courseID].items():
                abs_file_path = (
                    os.path.join(
                        self.downloadDir, f"{self.courseCode[courseID]}{fileName}"
                    )
                    .replace("\\", "/")
                    .replace("//", "/")
                )
                local_created_time = int(os.path.getctime(abs_file_path))
                try:
                    newPath = os.path.join(
                        os.path.dirname(abs_file_path),
                        f"{local_created_time}_{os.path.basename(abs_file_path)}",
                    )
                    if not self.config["keep_older_version"]:
                        os.remove(abs_file_path)
                    elif not os.path.exists(newPath):
                        os.rename(abs_file_path, newPath)
                    else:
                        pass
                        # abs_file_path = os.path.join(
                        #     os.path.dirname(abs_file_path),
                        #     f"{int(time.time())}_{os.path.basename(abs_file_path)}",
                        # )
                except Exception as e:
                    print(f"\t [{e.__class__.__name__}] Skipped: {abs_file_path}")

    def prepareDownload(self):
        for courseID in self.newFiles.keys():
            if courseID not in self.downloadList:
                self.downloadList[courseID] = {}
            self.downloadList[courseID].update(self.newFiles[courseID])
            self.downloadList[courseID].update(self.laterFiles[courseID])
        for courseID in self.downloadList.keys():
            for file_name, file_info in self.downloadList[courseID].items():
                self.downloadSize += file_info["size"]

    def categorizeFiles(self):
        for courseID in self.courseCode.keys():
            if courseID not in self.laterFiles:
                self.laterFiles[courseID] = {}
            if courseID not in self.newFiles:
                self.newFiles[courseID] = {}
            for file_name, file_info in self.onlineFiles[courseID].items():
                if file_name in self.localFiles[courseID]:
                    if (
                        file_info["modified_time"]
                        > self.localFiles[courseID][file_name]["modified_time"]
                    ):
                        self.laterFiles[courseID][file_name] = file_info
                    else:
                        pass
                        # print(
                        # f"{self.courseCode[courseID]}{file_name} has newer local version. Removed from download list."
                        # )
                else:
                    self.newFiles[courseID][file_name] = file_info

    def checkFileTypeHelper(self, courseID, filename):
        fileType = (mimetypes.guess_type(filename))[0]
        if fileType is None:
            return True
        if not self.config["allowAudio"]:
            if fileType.split("/")[0] == "audio":
                print(f"Audio removed: {self.courseCode[courseID]}{filename} ")
                return False
        if not self.config["allowVideo"]:
            if fileType.split("/")[0] == "video":
                print(f"Video removed: {self.courseCode[courseID]}{filename} ")
                return False
        if not self.config["allowImage"]:
            if fileType.split("/")[0] == "image":
                print(f"Image removed: {self.courseCode[courseID]}{filename} ")
                return False
        return True

    def checkFileType(self):
        for courseID in self.courseCode.keys():
            items = list(self.onlineFiles[courseID].items())
            for file_name, file_info in items:
                if not self.checkFileTypeHelper(courseID, file_name):
                    self.onlineFiles[courseID].pop(file_name)

    def countFiles(self, filesDict):
        count = 0
        for courseID in filesDict.keys():
            count += len(filesDict[courseID])
        return count

    def checkFileSize(self):
        for courseID in self.courseCode.keys():
            if courseID not in self.overSizedFiles:
                self.overSizedFiles[courseID] = {}
            items = list(self.onlineFiles[courseID].items())
            for file_name, file_info in items:
                if file_info["size"] > self.config["filesizeThresh"] * 1024 * 1024:
                    self.overSizedFiles[courseID][file_name] = file_info
                    self.onlineFiles[courseID].pop(file_name)

    async def sync(self):
        # Get course IDs
        times = 0
        total_course_number = len(self.config.get("courseCodes", [])) + len(
            self.config.get("courseIDs", [])
        )
        print("Getting course IDs...")
        while times < 5:
            await self.getCourseID()
            if len(self.courseCode) == total_course_number:
                break
            else:
                print(f"Number of available courses doesn't match, retrying...")
            times += 1
        print(f"Get {len(self.courseCode)} available courses.\n")

        # Get files
        print("Finding files on canvas...")
        # Get online files and folders
        await asyncio.gather(
            *[
                asyncio.create_task(self.getCourseFiles(courseID))
                for courseID in self.courseCode.keys()
            ]
        )
        # Get offline files
        for courseID in self.courseCode.keys():
            self.localFiles[courseID] = self.scanLocalFiles(
                courseID, self.folders[courseID]
            )

        print(f"Found {self.countFiles(self.onlineFiles)} files on canvas.")

        # Check file size
        self.checkFileSize()
        self.checkFileType()
        self.categorizeFiles()
        self.checkLaterFiles()
        self.prepareDownload()

        if not self.downloadList:
            return print("All local files are synced!")
        else:
            print(f"Prepare to download {self.countFiles(self.downloadList)} files.")
            await self.client.downloadMany(
                self.downloadDir, self.downloadList, self.downloadSize, self.courseCode
            )
            print(f"Sync completed!")

        return
