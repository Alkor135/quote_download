"""
Удаление пустых файлов в заданной папке
"""

from pathlib import Path
import glob


def del_file(path_file_lst: list) -> None:
    """
    Функция удаляет файлы с размером 0
    :param path_file_lst: Список путей с файлами
    :return: None
    """
    count_del: int = 0  # Счетчик удаленных файлов
    count_file: int = 0  # Счетчик всех файлов

    # Удаление файлов размером меньше 1 килобайта
    for file_path in path_file_lst:
        if file_path.is_file() and file_path.stat().st_size < 1024:
            try:
                file_path.unlink()  # Удаляем файл
                count_del += 1
            except Exception as err:
                print(f'Ошибка удаления файла: {err}')
        count_file += 1
    print(f'Из {count_file:,} файлов, удалено {count_del:,} пустых файлов')


def get_size_dir(path_file_lst: list, dir_storage: Path) -> None:
    """
    Функция подсчитывает общий объем всех файлов в каталоге и подкаталогах
    :param dir_storage: Путь к обрабатываемому каталогу
    :param path_file_lst: Список путей с файлами
    :return: None
    """
    total_file_size: int = 0  # Общий объем файлов
    count_file: int = 0  # Общий счетчик файлов
    for file in path_file_lst:
        total_file_size += file.stat().st_size
        count_file += 1
    print(f'Папка {dir_storage} содержит {count_file:,} файлов общим объемом {total_file_size // 1000000:,} MB')


if __name__ == "__main__":

    path_storage: Path = Path(r'c:\data_quote\data_finam_RTS_tick')  # Путь по которому нужно стереть пустые файлы
    # path_storage: Path = Path(r'c:\data_quote\data_finam_BR_tick')  # Путь по которому нужно стереть пустые файлы

    file_lst: list = glob.glob(f'{path_storage}/*')  # Создание списка файлов
    # print(file_lst)
    path_lst: list = [Path(file) for file in file_lst]  # Создание списка путей
    # print(file_lst)

    get_size_dir(path_lst, path_storage)  # Вывод информации по файлам и каталогу
    del_file(path_lst)  # Вызов функции удаления нулевых файлов
