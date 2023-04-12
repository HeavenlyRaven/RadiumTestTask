"""Asynchronously download files from a Gitea repo's HEAD and hash them."""
import asyncio
import hashlib
from io import FileIO
from pathlib import Path
from typing import Coroutine, Final

import aiohttp

# CONFIGURABLE CONSTANTS
REPO_OWNER: Final[str] = 'radium'
REPO_NAME: Final[str] = 'project-configuration'
GITEA_SOURCE_URL: Final[str] = 'https://gitea.radium.group'
# Relative paths are used here for polaceholders.
# Please, refrain from using relative paths when actually using the script.
TMP_DIR_PATH: Final[Path] = Path(
    'tmp/{repo_owner}_{repo_name}_HEAD'.format(
        repo_owner=REPO_OWNER,
        repo_name=REPO_NAME,
    ),
)
HASH_FILE_PATH: Final[Path] = Path(
    'tmp/{repo_owner}_{repo_name}_HASH.txt'.format(
        repo_owner=REPO_OWNER,
        repo_name=REPO_NAME,
    ),
)
# AUXILARY URL CONSTANTS (DO NOT TOUCH)
API_URL: Final[str] = '{source}/api/v1/repos/{repo_owner}/{repo_name}'.format(
    source=GITEA_SOURCE_URL,
    repo_owner=REPO_OWNER,
    repo_name=REPO_NAME,
)
TREE_API_URL: Final[str] = '{api}/git/trees/HEAD?recursive=true'.format(
    api=API_URL,
)


async def load_file(session: aiohttp.ClientSession, filepath: str) -> None:
    """Download a file from the repository to the temporary folder.

    Args:
        session: aiohttp.ClientSession
            client session to use
        filepath: str
            file path string
    """
    raw_api_url = '{api}/raw/{filepath}?ref=HEAD'.format(
        api=API_URL,
        filepath=filepath,
    )
    async with session.get(raw_api_url) as response:
        download_path = Path(TMP_DIR_PATH, filepath)
        # TODO: optimize later
        download_path.parent.mkdir(parents=True, exist_ok=True)
        # generally risky but okay for this case (under 1KB of data)
        download_path.write_bytes(await response.read())


async def load_tree(session: aiohttp.ClientSession) -> dict:
    """Get tree data from the repository as Python dict.

    Args:
        session: aiohttp.ClientSession
            client session to use

    Returns:
        dict
            GIT tree object as a Python dict
    """
    async with session.get(TREE_API_URL) as response:
        return await response.json()


def get_filepaths(tree: dict) -> list[str]:
    """Get str paths of all files in GIT tree.

    Args:
        tree: dict
            GIT tree object as a Python dict

    Returns:
        list[str]
            list of file paths
    """
    return [node['path'] for node in tree['tree'] if node['type'] != 'tree']


def get_hexhash(file_to_hash: FileIO, bufsize: int = 262144) -> str:
    """Get a SHA256 hexhash of a file.

    Args:
        file_to_hash: FileIO
            str path of a file to hash
        bufsize: int
            buffer size (default copied from 3.11's file_digest)

    Returns:
        str
            hexadecimal hash str
    """
    buf = bytearray(bufsize)
    view = memoryview(buf)
    digest = hashlib.sha256()
    while True:
        size = file_to_hash.readinto(buf)
        if not size:
            break
        digest.update(view[:size])
    return digest.hexdigest()


def hash_files(filepaths: list[Path]) -> None:
    """Hash files and record it in the hash file.

    Args:
        filepaths: list[Path]
            list of paths of files to hash
    """
    for filepath in filepaths:
        # disabling buffering to avoid double buffering (?)
        with filepath.open('rb', buffering=0) as file_to_hash:
            hexhash = get_hexhash(file_to_hash)
        with HASH_FILE_PATH.open('a') as hash_file:
            hash_file.write(
                '{file}: {hash}\n'.format(file=filepath, hash=hexhash),
            )


async def batcher(batch: list[Coroutine]) -> None:
    """Concurrently run all coroutines in the batch.

    Args:
        batch: list[Coroutine]
            list of coroutines to run concurrently
    """
    await asyncio.gather(*batch)


async def main() -> None:
    """Enter the event loop and start script."""
    async with aiohttp.ClientSession() as session:
        filepaths = get_filepaths(await load_tree(session))
        # populating 3 batches with loading coros to wrap them in tasks
        batches = ([], [], [])
        for index, filepath in enumerate(filepaths):
            batches[index % 3].append(load_file(session, filepath))
        # using gather instead of a task group for 3.9 compatibility
        await asyncio.gather(
            *[asyncio.create_task(batcher(batch)) for batch in batches],
        )
    # TODO: probably needs a little optimization
    hash_files([Path(TMP_DIR_PATH, fpath) for fpath in filepaths])


if __name__ == '__main__':
    asyncio.run(main())
