"""Download files with progress bar.

Taken from examples in https://github.com/Textualize/rich
"""

import signal
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from threading import Event
from typing import Iterable
from urllib.request import urlopen

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),  # noqa: FS003
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",  # noqa: FS003
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)
done_event = Event()


def handle_sigint(signum, frame):
    """Handle SIGINT (Ctrl+C) signal."""
    done_event.set()


signal.signal(signal.SIGINT, handle_sigint)


def copy_url(task_id: TaskID, url: str, path: str) -> None:
    """Copy data from a url to a local file."""
    response = urlopen(url)
    # This will break if the response doesn't contain content length
    progress.update(task_id, total=int(response.info()["Content-length"]))
    with open(path, "wb") as dest_file:
        progress.start_task(task_id)
        for data in iter(partial(response.read, 32768), b""):
            dest_file.write(data)
            progress.update(task_id, advance=len(data))
            if done_event.is_set():
                return
    progress.console.log(f"Downloaded {path}")


def download(urls: Iterable[str], dest_dir: str):
    """Download multuple files to the given directory."""
    with progress, ThreadPoolExecutor(max_workers=4) as pool:
        for url in urls:
            filename = url.split("/")[-1]
            dest_path = Path(dest_dir, filename)
            task_id = progress.add_task("download", filename=filename, start=False)
            pool.submit(copy_url, task_id, url, dest_path)
