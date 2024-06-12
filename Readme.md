# File indexer

### What is this for?
This module for indexing and searching *by line number and only line number!* large files.  

After indexing, a `.ind` file appears next to the file, which is an index file,
each value of which is a number containing the byte number of the end of each line of the original file.  
Each cell of the indexed file takes 8 bytes and if necessary,
find, for example, `1_700_000` line is needed `8 * (1_700_000 -1)`
the result will be the byte number from which the desired line begins (1_700_000).  
Next, mmap is used to quickly jump to this line.

### Classes definition

`FileIndexer` - Class for Indexing
`IndexedFiles` - Class for using search operation for several files (it receive list of already indexed files). It can be used for parallel datasets.

***
## *Ru doc*

## Описание класса  IndexedFiles
Этот класс `IndexedFiles` упрощает управление несколькими проиндексированными файлами. Он предоставляет методы для индексации файлов, доступа к предложениям по индексу и извлечения предложений в указанном диапазоне.

### Атрибуты:
- `_paths_files`: Список разрешенных объектов `Path`, представляющих пути к файлам.
- `_files_index`: Список объектов `IndexingDatasets`, представляющих проиндексированные файлы.
- `_is_indexed_all_files`: Булево значение, указывающее, проиндексированы ли все файлы.
- `required_index`: Список объектов `IndexingDatasets`, требующих индексации.

### Методы:
- `size_not_index_files()`: Возвращает общий размер файлов, которые требуют индексации.
- `is_indexed`: Проверяет, проиндексированы ли все файлы, и при необходимости инициирует процесс индексации для требуемых файлов.
- `wait_process_index()`: Ожидает завершения процесса индексации.
- `get_sentence_with_index(start: Union[int, None], finish: Union[int, None]) -> Sequence`: Извлекает предложения вместе с соответствующими номерами строк в указанном диапазоне.
- `get_sentence(start: Union[int, None], finish: Union[int, None]) -> Sequence`: Извлекает предложения в указанном диапазоне.
- `__len__()`: Возвращает общее количество строк в проиндексированных файлах.
- `__getitem__(item)`: Извлекает предложения по индексу или в указанном диапазоне.


## Описание класса IndexingDatasets
Класс `IndexingDatasets` предназначен для индексации одного конкретного файла с целью быстрого доступа к его содержимому. Вот описание его методов и атрибутов:

### Атрибуты:

1.  `_path_file: Path`: Путь к индексируемому файлу.
2.  `_path_index: Path`: Путь к каталогу, где хранится индексный файл.
3.  `is_in_process_of_indexing: bool`: Флаг, указывающий на наличие процесса индексации для файла.
4.  `is_indexed: bool`: Флаг, указывающий, был ли файл проиндексирован.
5.  `_fileindex_path_indexing: Path`: Путь к временному индексному файлу в процессе индексации.
6.  `_fileindex_path: Path`: Путь к основному индексному файлу.
7.  `_io_open_file: BinaryIO`: Файловый объект для чтения индексируемого файла.
8.  `_open_file: mmap`: Объект mmap для доступа к содержимому индексируемого файла.
9.  `_io_open_file_index: BinaryIO`: Файловый объект для работы с индексным файлом.
10.  `_open_file_index: mmap`: Объект mmap для доступа к содержимому индексного файла.
11.  `_count_of_lines_file: int`: Количество строк в индексном файле.
12.  `_count_bytes_files: int`: Размер индексируемого файла в байтах.

### Методы:

    
1.  `__enter__(self) -> "IndexingDatasets"`: Метод для входа в контекст, открывает файлы и подготавливает объект к использованию.
    
2.  `__exit__(self, exc_type: Union[type, None] = None, exc_value: Union[BaseException, None] = None, tb: Union[TracebackType, None] = None) -> None`: Метод для выхода из контекста, закрывает файлы.
    
3.  `__getitem__(self, item: slice) -> Union[Sequence[str], str]`: Метод получения строки(или строк) из файла по индексу(индексам).
    
4.  `__len__(self) -> int`: Метод возвращает общее количество строк в файле.
    
5.  `make_indexing(self, force: bool = False) -> None`: Метод для создания индекса файла для быстрого доступа к строкам.
    
6.  `get_line_offset_in_bytes(self, line_number: int) -> int`: Метод возвращает смещение в байтах для указанной строки.
    
7.  `line(self, line_number: int) -> str`: Метод возвращает строку по указанному номеру.
    
8.  `lines(self, start_line: int, finish_line: int, step: int = 1) -> Sequence[str]`: Метод возвращает строки в указанном диапазоне с заданным шагом.
    
9.  `is_required_indexing(self) -> bool`: Метод проверяет, требуется ли переиндексация файла.
    
10.  `set_line_by_index(self, line: str, index: int) -> None`: Метод для изменения строки по указанному индексу.
    
11.  `delete_line_by_index(self, index: int) -> None`: Метод для удаления строки по указанному индексу.
    
12.  `rechecking_indexed_file(self) -> None`: Метод для повторной проверки состояния индексации файла.
    
13.  `remove_temporary_files(path: str) -> None`: Статический метод для удаления временных файлов, созданных во время индексации.
