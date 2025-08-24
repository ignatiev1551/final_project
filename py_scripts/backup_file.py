import shutil
import os
from pathlib import Path

def backup_file(filepath):
    """
    Функция добавления к имени файла с расширением .backup и последующее перемещение его в католог с именем archive
    """
    try:
        source_filepath=Path(filepath)
        destination_filepath=Path("..")/"archive"/f"{source_filepath.name}.backup"
        if not os.path.exists(source_filepath):
            raise FileNotFoundError(f"Указанный путь {source_filepath} не существует")
        if not os.path.exists(destination_filepath.parent):
            raise FileNotFoundError(f"Указанный путь {destination_filepath.parent} не существует")
        shutil.move(str(source_filepath), str(destination_filepath))
    except Exception as e:
        print(e)        