"""Realdebrid module"""

import contextlib
import time
from datetime import datetime
from os.path import splitext
from pathlib import Path
from types import SimpleNamespace
from typing import Generator, List

from .shared import FileFinder
from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.media.state import States
from program.media.stream import Stream
from program.settings.manager import settings_manager
from requests import ConnectTimeout
from RTN.exceptions import GarbageTorrent
from RTN.parser import parse
from RTN.patterns import extract_episodes
from utils.logger import logger
from utils.ratelimiter import RateLimiter
from utils.request import get, ping, post


RD_BASE_URL = "https://api.real-debrid.com/rest/1.0"


class RealDebridDownloader:
    """Real-Debrid API Wrapper"""

    def __init__(self):
        self.rate_limiter = None
        self.key = "realdebrid"
        self.settings = settings_manager.settings.downloaders.real_debrid
        self.download_settings = settings_manager.settings.downloaders
        self.auth_headers = {"Authorization": f"Bearer {self.settings.api_key}"}
        self.proxy = self.settings.proxy_url if self.settings.proxy_enabled else None
        self.torrents_rate_limiter = RateLimiter(1, 1)
        self.overall_rate_limiter = RateLimiter(60, 60)
        self.finder = FileFinder(
            "filename",
            "filesize",
            self.download_settings.episode_filesize_min * 1_000_000,
            self.download_settings.episode_filesize_max * 1_000_000 if self.download_settings.episode_filesize_max != -1 else float("inf"))
        self.initialized = self.validate()
        if not self.initialized:
            return
        logger.success("Real Debrid initialized!")

    def validate(self) -> bool:
        """Validate Real-Debrid settings and API key"""
        if not self.settings.enabled:
            logger.warning("Real-Debrid is set to disabled")
            return False
        if not self.settings.api_key:
            logger.warning("Real-Debrid API key is not set")
            return False
        if not isinstance(self.download_settings.movie_filesize_min, int) or self.download_settings.movie_filesize_min < -1:
            logger.error("Real-Debrid movie filesize min is not set or invalid.")
            return False
        if not isinstance(self.download_settings.movie_filesize_max, int) or self.download_settings.movie_filesize_max < -1:
            logger.error("Real-Debrid movie filesize max is not set or invalid.")
            return False
        if not isinstance(self.download_settings.episode_filesize_min, int) or self.download_settings.episode_filesize_min < -1:
            logger.error("Real-Debrid episode filesize min is not set or invalid.")
            return False
        if not isinstance(self.download_settings.episode_filesize_max, int) or self.download_settings.episode_filesize_max < -1:
            logger.error("Real-Debrid episode filesize max is not set or invalid.")
            return False
        if self.settings.proxy_enabled and not self.settings.proxy_url:
            logger.error("Proxy is enabled but no proxy URL is provided.")
            return False
        try:
            response = ping(
                f"{RD_BASE_URL}/user",
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                overall_rate_limiter=self.overall_rate_limiter)
            if response.is_ok:
                user_info = response.response.json()
                expiration = user_info.get("expiration", "")
                expiration_datetime = datetime.fromisoformat(expiration.replace("Z", "+00:00")).replace(tzinfo=None)
                time_left = expiration_datetime - datetime.utcnow().replace(tzinfo=None)
                days_left = time_left.days
                hours_left, minutes_left = divmod(time_left.seconds // 3600, 60)
                expiration_message = ""

                if days_left > 0:
                    expiration_message = f"Your account expires in {days_left} days."
                elif hours_left > 0:
                    expiration_message = f"Your account expires in {hours_left} hours and {minutes_left} minutes."
                else:
                    expiration_message = "Your account expires soon."

                if user_info.get("type", "") != "premium":
                    logger.error("You are not a premium member.")
                    return False
                else:
                    logger.log("DEBRID", expiration_message)

                return user_info.get("premium", 0) > 0
        except ConnectTimeout:
            logger.error("Connection to Real-Debrid timed out.")
        except Exception as e:
            logger.exception(f"Failed to validate Real-Debrid settings: {e}")
        except:
            logger.error("Couldn't parse user data response from Real-Debrid.")
        return False

    def run(self, item: MediaItem) -> bool:
        """Download media item from real-debrid.com"""
        return_value = False
        for stream in item.streams:
            if item.is_stream_blacklisted(stream):
                continue
            file_dict = self.is_cached(item, [stream.infohash], True)
            if file_dict:
                logger.log(
                    "DEBRID", f"Item has cached containers, proceeding with: {item.log_string}"
                )
                self.download(item, stream.infohash, file_dict[stream.infohash])
                return_value = True
                break
            else:
                logger.log(
                    "DEBUG", f"Blacklisting uncached hash ({stream.infohash}) for item: {item.log_string}"
                )
                item.blacklist_stream(stream)
        return return_value
    
    def get_cached_hashes(self, item: MediaItem, streams: list[str]) -> list[str]:
        """Check if the item is cached in real-debrid"""
        return self.is_cached(item, streams, False) 
    
    def is_cached(self, item, streams, break_on_first=True) -> bool:
        return_dict = {}
        cached_hashes = self.get_instant_availability(streams)
        if cached_hashes:
            for hash, provider in cached_hashes.items():
                if type(provider) != dict:
                    continue
                for container_list in provider.values():
                    for container in container_list:
                        _container = [{**file, 'id': file_id} for file_id, file in container.items()]
                        files = self.finder.find_required_files(item, _container)
                        if files:
                            return_dict[hash] = files
                            if break_on_first:
                                return return_dict
                            break

        return return_dict     

    def download(self, item: MediaItem, stream: str, container) -> None:
        torrent_id = self.torrent_is_downloaded(item, stream) 
        if torrent_id:
            self.set_active_files(item, torrent_id, container)
        else:
            torrent_id = self.add_magnet(stream)
            time.sleep(1)
            self.select_files(torrent_id, container)
            self.set_active_files(item, torrent_id, container=container)
        self.active_stream = stream
        logger.log("DEBUG", f"Downloaded {item.log_string}")

    def download_cached(self, item: MediaItem, stream: str) -> None:
        """ I assume that the item has been checked for cached files before calling this function """
        added_magnet = False
        torrent_id = self.torrent_is_downloaded(item, stream)
        if not torrent_id:
            torrent_id = self.add_magnet(stream)
            time.sleep(1)
            added_magnet = True
        info = self.get_torrent_info(torrent_id)
        files = [{"filename": Path(file["path"]).name, "filesize": file["bytes"], 'id': file["id"]} for file in info["files"]]
        container = self.finder.find_required_files(item, files)
        if added_magnet:
            self.select_files(torrent_id, container)
        self.set_active_files(item, torrent_id, container=container)
        self.active_stream = stream
        logger.log("DEBUG", f"Downloaded {item.log_string}")
                
    def torrent_is_downloaded(self, item, hash_key) -> int:
        """Check if item is already downloaded after checking if it was cached
        return torrent id if it is downloaded with correct files"""
        logger.debug(f"Checking if torrent is already downloaded for item: {item.log_string}")
        torrents = self.get_torrents(1000)
        torrent = torrents.get(hash_key)

        if not torrent:
            logger.debug(f"No matching torrent found for hash: {hash_key}")
            return None

        info = self.get_torrent_info(torrent.id)
        if not info["files"]:
            logger.debug(f"Failed to get torrent info for ID: {torrent.id}")
            return None

        files = [{"filename": Path(file["path"]).name, "filesize": file["bytes"], 'id': file["id"]} for file in info["files"] if file["selected"]]
        if not self.finder.find_required_files(item, files):
            return None

        return torrent.id
    
    def set_active_files(self, item: MediaItem, torrent_id, container = None) -> None:
        """Set active files to media item"""
        info = self.get_torrent_info(torrent_id)
        if not container:
            container = self.finder.find_required_files(item, info["files"])
        if container:
            if item.type == "movie":
                file = self.finder.find_required_files(item, container)[0]
                _file_path = Path(file["filename"])
                item.set("folder", info["filename"])
                item.set("alternative_folder", info["original_filename"])
                item.set("file", _file_path.name)
            if item.type == "show":
                files = self.finder.find_required_files(item, container)
                for season in item.seasons:
                    for episode in season.episodes:
                        file = self.finder.find_required_files(episode, files)
                        if file:
                            file = file[0]
                            _file_path = Path(file["filename"])
                            episode.set("folder", info["filename"])
                            episode.set("alternative_folder", info["original_filename"])
                            episode.set("file", _file_path.name)

            if item.type == "season":
                files = self.finder.find_required_files(item, container)
                for episode in item.episodes:
                    file = self.finder.find_required_files(episode, files)
                    if file:
                        file = file[0]
                        _file_path = Path(file["filename"])
                        episode.set("folder", info["filename"])
                        episode.set("alternative_folder", info["original_filename"])
                        episode.set("file", _file_path.name)
            if item.type == "episode":
                file = self.finder.find_required_files(item, container)[0]
                _file_path = Path(file["filename"])
                item.set("folder", info["filename"])
                item.set("alternative_folder", info["original_filename"])
                item.set("file", _file_path.name)

    ### API Methods for Real-Debrid below

    def add_magnet(self, hash: str) -> str:
        """Add magnet link to real-debrid.com"""
        try:
            response = post(
                f"{RD_BASE_URL}/torrents/addMagnet",
                {"magnet": f"magnet:?xt=urn:btih:{hash}&dn=&tr="},
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                specific_rate_limiter=self.torrents_rate_limiter,
                overall_rate_limiter=self.overall_rate_limiter
            )
            if response.is_ok:
                return response.data.id
            logger.error(f"Failed to add magnet: {response.data}")
        except Exception as e:
            logger.error(f"Failed to add magnet: {e}")
        return None
    
    def get_instant_availability(self, hashes: List[str]) -> dict:
        """Get instant availability from real-debrid.com"""
        if not hashes:
            logger.error("No hashes found")
            return {}

        try:
            response = get(
                f"{RD_BASE_URL}/torrents/instantAvailability/{'/'.join(hashes)}",
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                specific_rate_limiter=self.torrents_rate_limiter,
                overall_rate_limiter=self.overall_rate_limiter,
                response_type=dict
            )
            if response.is_ok:
                return response.data
        except Exception as e:
            logger.error(f"Error getting instant availability for {hashes or 'UNKNOWN'}: {e}")
        return {}

    def get_torrent_info(self, request_id: str) -> dict:
        """Get torrent info from real-debrid.com"""
        if not request_id:
            logger.error("No request ID found")
            return {}

        try:
            response = get(
                f"{RD_BASE_URL}/torrents/info/{request_id}",
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                specific_rate_limiter=self.torrents_rate_limiter,
                overall_rate_limiter=self.overall_rate_limiter,
                response_type=dict
            )
            if response.is_ok:
                return response.data
        except Exception as e:
            logger.error(f"Error getting torrent info for {request_id or 'UNKNOWN'}: {e}")
        return {}

        """Select files from real-debrid.com"""
    def select_files(self, request_id: str, container) -> bool:
        try:
            response = post(
                f"{RD_BASE_URL}/torrents/selectFiles/{request_id}",
                {"files": ",".join(str(file["id"]) for file in container)},
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                specific_rate_limiter=self.torrents_rate_limiter,
                overall_rate_limiter=self.overall_rate_limiter
            )
            return response.is_ok
        except Exception as e:
            logger.error(f"Error selecting files: {e}")
            return False

    def get_torrents(self, limit: int) -> dict[str, SimpleNamespace]:
        """Get torrents from real-debrid.com"""
        try:
            response = get(
                f"{RD_BASE_URL}/torrents?limit={str(limit)}",
                additional_headers=self.auth_headers,
                proxies=self.proxy,
                specific_rate_limiter=self.torrents_rate_limiter,
                overall_rate_limiter=self.overall_rate_limiter
            )
            if response.is_ok and response.data:
                return {torrent.hash: torrent for torrent in response.data}
        except Exception as e:
            logger.error(f"Error getting torrents from Real-Debrid: {e}")
        return {}