import os
import time
from contextlib import ExitStack
from dataclasses import dataclass
from enum import Enum
from logging import getLogger, StreamHandler, DEBUG
from multiprocessing import Process, Queue, cpu_count, Manager, Condition, current_process
from queue import Empty
from typing import Final, Union, Sequence, BinaryIO, Type, Self
from types import TracebackType

from mmap import mmap, ACCESS_READ, PAGESIZE, MADV_DONTNEED, ACCESS_WRITE

from abc import ABC, abstractmethod
from pathlib import Path

import numpy

logger = getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel(DEBUG)

_EOL_BYTE: Final[bytes] = b"\n"
_INDEX_ITEM_BYTE_SIZE: Final[int] = 8
_ENCODING_UTF_8: Final[str] = "utf-8"
_MIN_SIZE_BLOCK: Final[int] = 10 ** 6
_PART_OF_DEFAULT_FILE: Final[int] = 5 * 10 ** 6
_FILE_SIZE_TO_WAIT_FOR_INDEXING: Final[int] = 200 * 10 ** 6


class IndexedDatasetStatus(Enum):
    INDEXING = -1
    NOT_INDEXED = 0
    INDEXED = 1


@dataclass(frozen=True, kw_only=True)
class _TaskSearchEOL:
    index_start: int
    index_end: int
    file: int


@dataclass
class _FileParseContext:
    finish: int
    size_file: int
    file_index: int
    start: int = 0


class NotIndexFile(Exception):
    """Raised when the input value is less than 18"""
    pass


class NotInterger(Exception):
    """That position is not an integer"""
    pass


class LessOne(Exception):
    """That position is less than 1"""
    pass


class BetterPages(Exception):
    """That position is better count pages"""
    pass


class NoEndOfLine(Exception):
    """That file have one line or don't end of line"""
    pass


class FileOrDirNotExist(Exception):
    """File does not exist"""
    pass


class FileChangeNow(Exception):
    """Someone is working with the file at the moment"""
    pass


class ProcessError(Exception):
    """Process does not exist"""
    pass


class FileIndexerAbstract(ABC):
    @abstractmethod
    def get_line_offset_in_bytes(self, line: int) -> int:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def line(self, line: int) -> str:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def lines(self, start: int, finish: int) -> Sequence:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def is_required_indexing(self) -> bool:
        raise NotImplementedError("Subclasses must implement this method")


class FileIndexer(FileIndexerAbstract):

    def __init__(self, path_file: str, path_index: str) -> None:
        self._path_file = Path(path_file).resolve()
        self._path_index = Path(path_index).resolve()

        if not self._path_file.is_file() or not self._path_index.is_dir():
            raise FileOrDirNotExist()

        self._fileindex_path_indexing: Path = self._path_index / (self._path_file.stem + ".rind")
        self._fileindex_path: Path = self._path_index / (self._path_file.stem + ".ind")

        self.is_in_process_of_indexing = self._fileindex_path_indexing.exists()
        self.is_indexed = self._is_valid_indexing_file()

    def __enter__(self) -> Self:
        self._io_open_file = open(self._path_file, "rb")
        self._open_file = mmap(self._io_open_file.fileno(), 0, access=ACCESS_READ)
        if self.is_indexed:
            self._io_open_file_index = open(self._fileindex_path, "r+b")
            self._open_file_index = mmap(self._io_open_file_index.fileno(), 0)
        return self

    def __exit__(
        self,
        exc_type: Union[type, None] = None,
        exc_value: Union[BaseException, None] = None,
        tb: Union[TracebackType, None] = None,
    ) -> None:
        if hasattr(self, "_open_file_index"):
            self._open_file_index.close()
            self._io_open_file_index.close()
        if hasattr(self, "_open_file"):
            self._open_file.close()
            self._io_open_file.close()

    def __getitem__(self, item: slice) -> Union[Sequence[str], str]:
        if isinstance(item, slice):
            start = 0 if item.start is None else item.start
            finish = self.count_of_lines if item.stop is None else item.stop
            step = 1 if item.step is None else item.step
            return self.lines(start, finish, step)
        elif isinstance(item, int):
            return self.line(item)
        else:
            raise TypeError("Invalid argument")

    def __len__(self) -> int:
        return self.count_of_lines

    def make_indexing(self, force: bool = False) -> None:
        if not force and (self.is_in_process_of_indexing or self.is_indexed):
            return
        _PAGESIZE = PAGESIZE
        __EOL_BYTE = _EOL_BYTE
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        __PART_OF_DEFAULT_FILE = _PART_OF_DEFAULT_FILE
        with open(self._fileindex_path_indexing, "wb") as file_index:
            file = self._file
            offset = 0
            file_index.write(offset.to_bytes(__INDEX_ITEM_BYTE_SIZE, "little", signed=False))
            prev_advise_offset = 0
            threshold_size_for_reset = __PART_OF_DEFAULT_FILE
            while (index := file.find(__EOL_BYTE, offset)) >= 0:
                offset = index + 1
                if offset != self._filesize:
                    file_index.write(int(offset).to_bytes(__INDEX_ITEM_BYTE_SIZE, "little", signed=False))
                if offset >= threshold_size_for_reset:
                    len_advise = (offset - prev_advise_offset) // _PAGESIZE * _PAGESIZE
                    file.madvise(MADV_DONTNEED, prev_advise_offset, len_advise)
                    prev_advise_offset += len_advise
                    threshold_size_for_reset += __PART_OF_DEFAULT_FILE
        self._fileindex_path_indexing.rename(self._fileindex_path_indexing.with_suffix('.ind'))
        self.is_indexed = True
        self.is_in_process_of_indexing = False

    @property
    def path_file(self) -> Path:
        return self._path_file

    @property
    def path_file_index_completed(self) -> Path:
        return self._fileindex_path

    @property
    def path_file_index(self) -> Path:
        return self._fileindex_path_indexing

    def is_required_indexing(self) -> bool:
        if self.is_in_process_of_indexing or self.is_indexed:
            return False
        return True

    def _is_valid_indexing_file(self) -> bool:
        if not self.is_in_process_of_indexing and self._fileindex_path.exists():
            return self._path_file.stat().st_mtime < self._fileindex_path.stat().st_mtime
        else:
            return False

    @property
    def _file_index(self) -> mmap:
        if not hasattr(self, "_open_file_index") or self._open_file_index.closed:
            self._io_open_file_index = open(self._fileindex_path, "r+b")
            self._open_file_index = mmap(self._io_open_file_index.fileno(), 0)
        return self._open_file_index

    @property
    def _file(self) -> mmap:
        if not hasattr(self, "_open_file") or self._open_file.closed:
            self._io_open_file = open(self._path_file, "rb")
            self._open_file = mmap(self._io_open_file.fileno(), 0, access=ACCESS_READ)
        return self._open_file

    @property
    def count_of_lines(self) -> int:
        if not hasattr(self, "_count_of_lines_file"):
            if not self._fileindex_path.exists():
                raise NotIndexFile()
            self._count_of_lines_file = self._fileindex_path.stat().st_size // _INDEX_ITEM_BYTE_SIZE
        return self._count_of_lines_file

    @property
    def _filesize(self) -> int:
        if not hasattr(self, "_count_bytes_files"):
            self._count_bytes_files = self._path_file.stat().st_size
        return self._count_bytes_files

    def get_size_file(self) -> int:
        return self._filesize

    def get_line_offset_in_bytes(self, line_number: int) -> int:
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        if self.is_in_process_of_indexing or not self.is_indexed:
            raise NotIndexFile()
        line_number = self._validate_position(line_number)
        offset_index_file = line_number * __INDEX_ITEM_BYTE_SIZE
        index_line: bytes = self._file_index[offset_index_file: offset_index_file + __INDEX_ITEM_BYTE_SIZE]
        return int.from_bytes(index_line, "little", signed=False)

    def line(self, line_number: int) -> str:
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        start = self.get_line_offset_in_bytes(line_number)
        if line_number + 1 == self.count_of_lines:
            finish = self._filesize
        else:
            offset_index_file = (line_number + 1) * __INDEX_ITEM_BYTE_SIZE
            end_line: bytes = self._file_index[offset_index_file: offset_index_file + __INDEX_ITEM_BYTE_SIZE]
            finish = int.from_bytes(end_line, "little", signed=False)
        return self._file[start:finish].decode(_ENCODING_UTF_8).strip()

    def lines(self, start_line: int, finish_line: int, step: int = 1) -> Sequence[str]:
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        start = self.get_line_offset_in_bytes(start_line)
        if step > 1:
            return [self.line(i) for i in range(start_line, finish_line, step)]
        if finish_line + 1 > self.count_of_lines:
            finish = self._filesize
        else:
            offset_index_file = finish_line * __INDEX_ITEM_BYTE_SIZE
            end_lines: bytes = self._file_index[offset_index_file: offset_index_file + __INDEX_ITEM_BYTE_SIZE]
            finish = int.from_bytes(end_lines, "little", signed=False)
        return self._file[start:finish].decode(_ENCODING_UTF_8).splitlines()

    def get_line_and_start_position(self, line_number: int) -> tuple[bytes, int]:
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE

        start_pos = self.get_line_offset_in_bytes(line_number)
        if line_number + 1 == self.count_of_lines:
            finish_pos = self._filesize
        else:
            offset_index_file = (line_number + 1) * __INDEX_ITEM_BYTE_SIZE
            end_line_in_bytes: bytes = self._file_index[offset_index_file:offset_index_file+__INDEX_ITEM_BYTE_SIZE]
            finish_pos = int.from_bytes(end_line_in_bytes, "little", signed=False)
        line = self._file[start_pos:finish_pos]

        return line, start_pos

    def _reopen_index_file(self) -> mmap:
        if hasattr(self, "_open_file_index"):
            self._open_file_index.close()
            self._io_open_file_index.close()
        return self._file_index

    def _reopen_file(self) -> mmap:
        if hasattr(self, "_open_file"):
            self._open_file.close()
            self._io_open_file.close()
        return self._file

    def rechecking_indexed_file(self) -> None:
        self._clear_cache_data_index_file()
        self.is_in_process_of_indexing = self._fileindex_path_indexing.exists()
        self.is_indexed = self._is_valid_indexing_file()

    def _write_to_temporary_index_file(
        self,
        file_index: BinaryIO | mmap,
        file_index_size: int,
        line_number: int,
        delta: int,
        remove: bool,
    ) -> None:
        _PAGESIZE = PAGESIZE
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        __PART_OF_DEFAULT_FILE = _PART_OF_DEFAULT_FILE
        _BIG_PART_OF_DEFAULT_FILE = __PART_OF_DEFAULT_FILE * 3
        with open(self._fileindex_path_indexing, "wb") as temporary_index_file:
            start_for_change = (line_number + 1) * __INDEX_ITEM_BYTE_SIZE
            start_data = 0
            data_finish = __PART_OF_DEFAULT_FILE if __PART_OF_DEFAULT_FILE <= start_for_change else start_for_change
            while start_data != start_for_change:
                temporary_index_file.write(self._file_index[start_data:data_finish])
                start_data = data_finish
                data_finish += __PART_OF_DEFAULT_FILE
                if data_finish > start_for_change:
                    data_finish = start_for_change
            start_index = start_for_change
            if remove:
                start_index += __INDEX_ITEM_BYTE_SIZE
            finish_index = start_index + _BIG_PART_OF_DEFAULT_FILE
            if finish_index > file_index_size:
                finish_index = file_index_size
            prev_advise_offset = 0
            while start_index != finish_index:
                processed_values = numpy.frombuffer(file_index[start_index:finish_index], dtype="<i8") - delta
                temporary_index_file.write(processed_values.tobytes())
                start_index = finish_index
                finish_index += _BIG_PART_OF_DEFAULT_FILE
                if finish_index > file_index_size:
                    finish_index = file_index_size
                len_advise = (finish_index - prev_advise_offset) // _PAGESIZE * _PAGESIZE
                file_index.madvise(MADV_DONTNEED, prev_advise_offset, len_advise)
                prev_advise_offset += len_advise

    def set_line_by_index(self, line: str, index: int) -> None:
        if not self.is_indexed:
            raise NotIndexFile()
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        __EOL_BYTE = _EOL_BYTE
        if not line.endswith("\n"):
            line += "\n"
        line = line.encode(_ENCODING_UTF_8)
        self._create_file_with_change(line=line, line_number=index)
        del self._count_bytes_files
        self._reopen_file()

        line_number = self._validate_position(index)
        file_index = self._file_index
        file_index_size = file_index.size()
        line_lenght = len(line)
        offset_change_sentence = int.from_bytes(
            self._file_index[line_number * __INDEX_ITEM_BYTE_SIZE: (line_number + 1) * __INDEX_ITEM_BYTE_SIZE],
            "little",
            signed=False
        )
        self._reopen_file()
        if hasattr(self, "_count_bytes_files"):
            del self._count_bytes_files
        if line_lenght:
            end_sentence = line_lenght + offset_change_sentence
        else:
            end_sentence = self._file.find(__EOL_BYTE, offset_change_sentence) + 1
        if end_sentence == 0:
            end_sentence = self._filesize
        if line_number == self.count_of_lines:
            offset_next_sentence = self._filesize
        else:
            offset_next_sentence = int.from_bytes(
                self._file_index[
                (line_number + 1) * __INDEX_ITEM_BYTE_SIZE: (line_number + 2) * __INDEX_ITEM_BYTE_SIZE],
                "little",
                signed=False
            )
        size_sentence = end_sentence - offset_change_sentence
        before_size_sentence = offset_next_sentence - offset_change_sentence
        if size_sentence != before_size_sentence:
            delta = before_size_sentence - size_sentence
            self._write_to_temporary_index_file(
                file_index=file_index,
                file_index_size=file_index_size,
                line_number=line_number,
                delta=delta,
                remove=False,
            )
            self._fileindex_path_indexing.rename(self._fileindex_path_indexing.with_suffix('.ind'))
            self._clear_cache_data_index_file()

        self.is_indexed = True
        self.is_in_process_of_indexing = False

    def delete_line_by_index(self, index: int) -> None:
        if not self.is_indexed:
            raise NotIndexFile()
        self._create_file_with_change(line=b"", line_number=index)
        del self._count_bytes_files
        self._reopen_file()

        line_number = self._validate_position(index)
        file_index = self._file_index
        file_index_size = file_index.size()
        start_for_change = line_number * _INDEX_ITEM_BYTE_SIZE
        part_index_file_16_bytes = file_index[start_for_change: start_for_change + 16]
        offset_position = int.from_bytes(part_index_file_16_bytes[0:8], "little", signed=False)
        if line_number + 1 == self.count_of_lines:
            offset_next_position = self._filesize
        else:
            offset_next_position = int.from_bytes(part_index_file_16_bytes[8:16], "little", signed=False)
        delta = offset_next_position - offset_position
        self._write_to_temporary_index_file(
            file_index=file_index,
            file_index_size=file_index_size,
            line_number=line_number,
            delta=delta,
            remove=True,
        )
        self._fileindex_path_indexing.rename(self._fileindex_path_indexing.with_suffix('.ind'))
        self._clear_cache_data_index_file()
        self.is_indexed = True
        self.is_in_process_of_indexing = False

    def _create_file_with_change(
        self,
        line: bytes,
        line_number: int,
    ) -> None:
        _PAGESIZE = PAGESIZE
        file_path = self._path_index / (self._path_file.stem + ".rtxt")
        if file_path.exists():
            raise FileChangeNow("Someone is working with the file at the moment")
        __PART_OF_DEFAULT_FILE = _PART_OF_DEFAULT_FILE
        with open(file_path, "wb") as temporary_file:
            file = self._file
            end_file = self._filesize
            start_to_change = self.get_line_offset_in_bytes(line_number)
            if line_number + 1 == self.count_of_lines:
                finish_to_change = end_file
            else:
                finish_to_change = self.get_line_offset_in_bytes(line_number + 1)
            start = 0
            finish = min(__PART_OF_DEFAULT_FILE, start_to_change)
            write_limit = start_to_change
            threshold_size_for_reset = __PART_OF_DEFAULT_FILE
            prev_advise_offset = 0
            while finish < end_file or start != finish:
                temporary_file.write(file[start:finish])
                if finish == start_to_change:
                    temporary_file.write(line)
                    start = finish_to_change
                    finish = start + __PART_OF_DEFAULT_FILE
                    if finish > end_file:
                        finish = end_file
                    write_limit = end_file
                    continue
                start = finish
                finish = min(finish + __PART_OF_DEFAULT_FILE, write_limit)
                if finish >= threshold_size_for_reset:
                    len_advise = (finish - prev_advise_offset) // _PAGESIZE * _PAGESIZE
                    file.madvise(MADV_DONTNEED, prev_advise_offset, len_advise)
                    prev_advise_offset += len_advise
                    threshold_size_for_reset += __PART_OF_DEFAULT_FILE
        file_path.rename(file_path.with_suffix(self._path_file.suffix))

    def _clear_cache_data_index_file(self) -> None:
        if hasattr(self, "_count_of_lines_file"):
            del self._count_of_lines_file
        if hasattr(self, "_count_bytes_files"):
            del self._count_bytes_files
        self._reopen_index_file()

    @staticmethod
    def fine_end_string(file: mmap, start_position: int) -> int:
        return file.find(_EOL_BYTE, start_position)

    @staticmethod
    def remove_temporary_files(path: str) -> None:
        path = Path(path)
        suffix_patterns = ["*.rtxt", "*.rind"]
        for pattern in suffix_patterns:
            for file in path.rglob(pattern):
                os.remove(str(file))
                logger.info(f"Temporary index file: {file} was delete ")

    def _validate_position(self, line_number: int) -> int:
        """Validate line"""
        try:
            line_number = int(line_number)
        except (TypeError, ValueError):
            raise NotInterger()
        if line_number < 0:
            raise LessOne()
        if line_number > self.count_of_lines - 1:
            raise BetterPages()
        return line_number


class IndexedFiles:
    class_index = FileIndexer

    def __init__(self, paths: Sequence[str]) -> None:
        if not paths:
            raise Exception("Sequence must not be empty")
        self._paths_files: list[Path] = [Path(path).resolve() for path in paths]
        self._files_index: list[FileIndexer] = [
            self._get_class_index(str(path), str(path.parent.resolve()))
            for path in self._paths_files
        ]
        self._is_indexed_all_files: bool = all([i.is_indexed for i in self._files_index])
        self.required_index: list[FileIndexer] = list(
            filter(lambda f: f.is_required_indexing(), self._files_index)
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Union[type, None] = None,
        exc_value: Union[BaseException, None] = None,
        tb: Union[TracebackType, None] = None,
    ) -> None:
        for file_index in self._files_index:
            file_index.__exit__()

    def size_not_index_files(self):
        return sum([file.get_size_file() for file in self.required_index])

    @property
    def _get_class_index(self) -> Type[FileIndexer]:
        return self.class_index

    @property
    def is_indexed(self) -> bool:
        if self.required_index:
            self.index_process = Process(target=IndexedFiles.run_indexed, args=(self._files_index,))
            self.index_process.start()
            if self.size_not_index_files() <= _FILE_SIZE_TO_WAIT_FOR_INDEXING:
                self.index_process.join()
                self.index_process.close()
                for file_index in self.required_index:
                    file_index.rechecking_indexed_file()
                self._is_indexed_all_files = True
        return self._is_indexed_all_files

    def wait_process_index(self) -> None:
        if not hasattr(self, "index_process"):
            raise ProcessError("Process does not exist")
        if self.index_process.is_alive():
            self.index_process.join()
            self.index_process.close()
            for file_index in self.required_index:
                file_index.rechecking_indexed_file()
            self._is_indexed_all_files = True

    @staticmethod
    def run_indexed(indexes_files: Sequence[FileIndexer]) -> None:
        starttime = time.time()
        PID: Final[int] = current_process().pid
        count_cpu: Final[int] = cpu_count()
        requiring_indexing: Final[Sequence[FileIndexer]] = tuple(
            filter(lambda f: f.is_required_indexing(), indexes_files))
        for file in requiring_indexing:
            logger.info(f"Start indexing PID: {PID}, files: {file.path_file}")
        count_files = len(requiring_indexing)
        with (
            ExitStack() as stack,
            Manager() as manager,
            mmap(-1, length=len(requiring_indexing) * _INDEX_ITEM_BYTE_SIZE,
                 access=ACCESS_WRITE) as memory_offset_file_now,
        ):
            worker_count: Final[int] = count_cpu - 1
            tasks_queue: Final[Queue] = manager.Queue()
            open_files: Final[Sequence[BinaryIO]] = tuple(
                stack.enter_context(open(file.path_file, "rb")) for file in requiring_indexing
            )
            mmap_files: Final[Sequence[mmap]] = tuple(
                stack.enter_context(mmap(f.fileno(), length=0, access=ACCESS_READ)) for f in open_files
            )
            write_conditions: Final[Sequence[Condition]] = tuple(manager.Condition() for _ in range(count_files))
            open_temporary_files = [
                stack.enter_context(open(file.path_file_index, "wb"))
                for file in requiring_indexing
            ]
            tasks_process_generator = Process(
                target=IndexedFiles.create_tasks,
                args=(
                    requiring_indexing,
                    tasks_queue,
                    worker_count,
                ),
            )
            tasks_process_generator.start()

            processes: Final[list[Process]] = []
            for _ in range(worker_count):
                process = Process(
                    target=IndexedFiles.worker,
                    args=(
                        mmap_files,
                        open_temporary_files,
                        write_conditions,
                        tasks_queue,
                        memory_offset_file_now,
                    ),
                )
                processes.append(process)
                process.start()

            for process in processes:
                process.join()
                process.close()
            for temporary_file_index in requiring_indexing:
                path_temporary_file = Path(temporary_file_index.path_file_index)
                path_temporary_file.rename(path_temporary_file.with_suffix('.ind'))
                logger.info(f"Finish indexing PID: {PID}, files: {path_temporary_file.stem}")
            tasks_process_generator.join()
            tasks_process_generator.close()
            print(time.time() - starttime, "time")

    @staticmethod
    def create_tasks(
        requiring_indexing: FileIndexer,
        tasks_queue: Queue,
        count_processes: int,
        block_size: int = _MIN_SIZE_BLOCK,
    ) -> None:
        max_block_data_for_handler = count_processes * block_size
        chunk_size = block_size
        file_parse_contexts = []
        for file in requiring_indexing:
            size_file = file.get_size_file()
            file_parse_contexts.append(
                _FileParseContext(
                    finish=min(chunk_size, size_file),
                    size_file=size_file,
                    file_index=len(file_parse_contexts),
                )
            )

        cur_context_index = 0
        context_len = len(file_parse_contexts)
        while context_len:
            file_parse_context = file_parse_contexts[cur_context_index]
            start = file_parse_context.start
            finish = file_parse_context.finish

            size_file = file_parse_context.size_file
            tasks_queue.put(
                _TaskSearchEOL(
                    index_start=start,
                    index_end=finish,
                    file=file_parse_context.file_index,
                )
            )
            start = finish
            finish = min(finish + chunk_size, size_file)
            if start != finish:  # check on finish file
                cur_context_index += 1
                file_parse_context.start = start
                file_parse_context.finish = finish
            else:
                del file_parse_contexts[cur_context_index]
                context_len -= 1
            if cur_context_index == context_len:
                cur_context_index = 0
            if chunk_size < max_block_data_for_handler:
                chunk_size = min(chunk_size + block_size, max_block_data_for_handler)

    @staticmethod
    def worker(
        read_files: list[mmap],
        write_index_files: list[BinaryIO],
        write_files_conditions: list[Condition],
        tasks_queue: Queue,
        share_memory: mmap,
    ) -> None:
        _PAGESIZE = PAGESIZE
        __INDEX_ITEM_BYTE_SIZE = _INDEX_ITEM_BYTE_SIZE
        __PART_OF_DEFAULT_FILE = _PART_OF_DEFAULT_FILE
        __EOL_BYTE = _EOL_BYTE

        def predicate_for_wait() -> bool:
            position_for_write = int.from_bytes(
                share_memory[start_memory: finish_memory],
                "little",
                signed=False,
            )
            return position_for_write == start_position

        while not tasks_queue.empty():
            try:
                task: _TaskSearchEOL = tasks_queue.get(timeout=1)
            except Empty:
                continue

            start_position = offset = task.index_start
            file_index = task.file
            finish_position = task.index_end
            write_index_file = write_index_files[file_index]
            file = read_files[file_index]
            prev_advise_offset = (start_position // _PAGESIZE + 1) * _PAGESIZE
            threshold_size_for_reset = start_position + __PART_OF_DEFAULT_FILE
            buffer = []
            if start_position == 0:
                buffer.append(0)
            while (index := file.find(__EOL_BYTE, offset, finish_position)) >= 0:
                offset = index + 1
                buffer.append(offset)
                if offset >= threshold_size_for_reset:
                    len_advise = (offset - prev_advise_offset) // _PAGESIZE * _PAGESIZE
                    file.madvise(MADV_DONTNEED, prev_advise_offset, len_advise)
                    prev_advise_offset += len_advise
                    threshold_size_for_reset += __PART_OF_DEFAULT_FILE
            write_file_condition = write_files_conditions[file_index]
            start_memory = file_index * __INDEX_ITEM_BYTE_SIZE
            finish_memory = start_memory + __INDEX_ITEM_BYTE_SIZE
            bytes_result = numpy.array(buffer, dtype="<i8").tobytes()
            del buffer
            with write_file_condition:
                write_file_condition.wait_for(predicate_for_wait)
                write_index_file.write(bytes_result)
                write_index_file.flush()
                share_memory[start_memory: finish_memory] = finish_position.to_bytes(8, "little", signed=False)
                write_file_condition.notify_all()

    def get_sentence_with_index(self, start: Union[int, None], finish: Union[int, None]) -> Sequence[tuple[int, ...]]:
        part_of_lines = (file[start:finish] for file in self._files_index)
        res = tuple((start+index, *lines) for index, lines in enumerate(zip(*part_of_lines)))
        return res

    def get_sentence(self, start: Union[int, None], finish: Union[int, None]) -> Sequence[tuple[str]]:
        part_of_lines = (file[start:finish] for file in self._files_index)
        return tuple(lines for lines in zip(*part_of_lines))

    def __len__(self):
        return self._files_index[0].count_of_lines

    def __getitem__(self, item) -> Sequence[tuple[str, ...]]:
        if isinstance(item, slice):
            start = 0 if item.start is None else item.start
            finish = len(self) if item.stop is None else item.stop
            return self.get_sentence(start, finish)
        elif isinstance(item, int):
            return self.get_sentence(item, item + 1)
