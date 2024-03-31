import pandas as pd
import subprocess
import requests
import tarfile
import logging
import os
import io

import luigi
from luigi.util import requires

# Задание.
# 
# Шаг 1:
#       Скачать supplementary-файлы датасета GSE68849 из GEO → cтраница датасета. Архив называется GSE68849_RAW.tar.
#       Как видим, здесь стандартный endpoint, в который параметром подставляется название датасета.
#       В задаче на скачивание имя датасета должно быть параметром.
#       Ссылку на скачивание можно захардкодить, однако, более корректным решением будет выполнить парсинг страницы и найти ссылку там.
#       Архив скачивайте в подготовленную для него папку. Структура папок на ваше усмотрение, но имейте ввиду, что она должна, с одной
#           стороны, просто быть прочитана алгоритмом, а с другой — быть понятной для человека.
#       Скачивать можно с помощью библиотеки wget для Python.
#       Если нужно запустить какой-то Bash-код, используйте библиотеку subprocess.
# Шаг 2:
#       После скачивания в папке появится tar-архив, содержимое которого — gzip-заархивированные файлы.
#       Нужно разархивировать общий архив, узнать, сколько в нём файлов, и как они называются, создать под каждый файл папку и разархивировать
#           его туда.
#       Имейте в виду, что датасет может быть устроен по-другому. Например, в нём может быть другое количество файлов в архиве, наименования
#           этих файлов также могут отличаться. Чем универсальнее будет пайплайн, тем лучше.
#       Текстовые файлы представляют собой набор из 4-х tsv-таблиц, каждая из которых обозначена хедером. Хедеры начинаются с символа
#       Название файла — на ваше усмотрение. Постарайтесь сделать его максимально понятным и лаконичным.
#       В словаре DFS будет содержаться 4 дата фрейма под соответствующими ключами.
# Шаг 3:
#       Здесь мы видим, что таблица Probes содержит очень много колонок, часть из которых — большие текстовые поля. Помимо полного файла с этой
#           таблицей сохраните также урезанный файл.
#       Из него нужно убрать следующие колонки: Definition, Ontology_Component, Ontology_Process, Ontology_Function, Synonyms, Obsolete_Probe_Idz
#           Probe_Sequence.
# Шаг 4:
#       Теперь мы имеем разложенные по папкам tsv-файлы с таблицами, которые удобно читать. Изначальный текстовый файл можно удалить, убедившись,
#           что все предыдущие этапы успешно выполнены.

SAVE_PATH = os.getcwd() + "/dataset"
DEFAULT_DATASET_NAME = 'GSE68849'

log = logging.getLogger('luigi-interface')

# Основные шаги пайплайна:
#   Загрузка данных (DownloadData): Инициирует загрузку архива с данными (GSE68849_RAW.tar) с сайта GEO.
#   Извлечение архивов (ArchivesExtract): Распаковывает скачанный tar-архив, сохраняя содержащиеся в нем gzip-архивированные файлы в отдельно созданные директории.
#   Распаковка файлов (ExtractFiles): Для каждого gzip-файла создается отдельная папка, куда происходит извлечение содержимого.
#   Разделение на таблицы (SeparateTables): Извлекает из текстовых файлов четыре tsv-таблицы, каждая из которых маркирована своим заголовком.
#   Обработка таблиц (TableProcessing): Специфичная для таблицы Probes, этот шаг удаляет определенные колонки, чтобы создать упрощенную версию файла.
#   Очистка рабочего окружения (ClearEnvironment): Последний шаг удаляет все ненужные файлы, оставляя только необходимые tsv-таблицы.

# Шаг 1.
#   Скачать supplementary-файлы датасета GSE68849 из GEO → cтраница датасета. Архив называется GSE68849_RAW.tar.
class DownloadData(luigi.Task):
    # В задаче на скачивание имя датасета должно быть параметром.
    # Ссылку на скачивание можно захардкодить.
    dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)
    dataset_link = 'https://www.ncbi.nlm.nih.gov/geo/download/?acc={dataset_name}&format=file'

    def output(self):
        return luigi.LocalTarget(f'{self.SAVE_PATH}/{self.dataset_name}.tar')

    def run(self):
        log.info(f"[INFO] Downloading archive {dataset_link} is in progress ...")
        dataset_link = self.link.format(dataset_name=self.dataset_name)

        response = requests.get(dataset_link)
        response.raise_for_status()

        log.info(f'[INFO] File downloaded successfully from {dataset_link}')

        with open(self.output().path, 'wb') as f:
            f.write(response.content)
            log.info(f'[INFO] Archive is saved to {self.output()}.')

# Шаг 2:
#   Нужно разархивировать общий архив, узнать, сколько в нём файлов, и как они называются,
#   создать под каждый файл папку и разархивировать его туда.
@requires(DownloadData)
class ArchivesExtract(luigi.Task):
    def output(self):
        if self.input():
            tar_file = self.input().path
            if os.path.exists(tar_file):
                tarf = tarfile.open(tar_file)
                files = tarf.getnames()
                return (luigi.LocalTarget(f"data/{self.dataset_name}/archives"), 
                       [luigi.LocalTarget(f"data/{self.dataset_name}/archives/{file_name}") for file_name in files])

    def run(self):
        os.makedirs(self.output()[0].path, exist_ok=True)

        log.info(f"[INFO] Extracting files from the archive into {self.output()[0].path} is in progress ...")
        with tarfile.open(self.input().path) as archive:
            archive.extractall(path=self.output()[0].path)

        log.info(f"[INFO] Files from tar archive are extracted into {self.output()[0].path}.")

# Шаг 2:
#   Нужно разархивировать общий архив, узнать, сколько в нём файлов, и как они называются,
#   создать под каждый файл папку и разархивировать его туда.
@requires(ArchivesExtract)        
class ExtractFiles(luigi.Task):
    def output(self):
        if self.input():
            extracted_dir = self.input()[0].path
            parent_dir_path = os.path.dirname(extracted_dir)      
            target_dir_path = os.path.join(parent_dir_path, 'files') 

            input_files = [file.path for file in self.input()[1]]
            output_files = []

            for file in input_files:
                if file.endswith('.gz'):
                    base_name = os.path.basename(file).rsplit('.', 2)[0]
                    SAVE_PATH = os.path.join(target_dir_path, base_name, os.path.basename(file)[:-3])
                    output_files.append(SAVE_PATH)

            return (luigi.LocalTarget(target_dir_path), [luigi.LocalTarget(file_name) for file_name in output_files])

    def run(self):
        target_dir_path = self.output()[0].path
        os.makedirs(target_dir_path, exist_ok=True)

        input_files = [file.path for file in self.input()[1] if file.path.endswith('.gz')]
        output_files = [file.path for file in self.output()[1]]

        for input, output in zip(input_files, output_files):
            base_name = os.path.basename(input).rsplit('.', 2)[0]
            os.makedirs(os.path.join(target_dir_path, base_name), exist_ok=True)
            log.info(f"[INFO] Unzipping archive to {base_name} is in progress ...")
            subprocess.run(['gunzip', '-c', input], stdout=open(output, 'wb'))
    
@requires(ExtractFiles)   
class SeparateTables(luigi.Task):
    def output(self):
        if self.input():
            tables = ['Columns.tsv',
                      'Controls.tsv',
                      'Heading.tsv',
                      'Probes.tsv']
            input_files = [file.path for file in self.input()[1]]
            output_files = []

            for file in input_files:
                parent_dir_path = os.path.dirname(file)
                for table in tables:
                    output_file = os.path.join(parent_dir_path, table)
                    output_files.append(output_file)

            return [luigi.LocalTarget(file) for file in output_files]
                
    def to_tables(self, file_path):
        DFS = {}
        with open(file_path) as f:
            write_key = None
            file_IO = io.StringIO()
            for lines in f.readlines():
                if lines.startswith('['):
                    if write_key:
                        file_IO.seek(0)
                        DFS[write_key] = pd.read_csv(file_IO, sep='\t', header=(None if write_key == 'Heading' else 'infer'))

                        log.info(f"[INFO] Saving dataframe {write_key} is in progress ...")
                        self.save_dataframe(DFS[write_key], file_path, write_key)
                    file_IO = io.StringIO()
                    write_key = lines.strip('[]\n')
                    continue
                if write_key:
                    file_IO.write(lines)
            if write_key:
                file_IO.seek(0)
                DFS[write_key] = pd.read_csv(file_IO, sep='\t')
                self.save_dataframe(DFS[write_key], file_path, write_key)

    def save_dataframe(self, df, original_file_path, table_name):
        file_name = f"{table_name}.tsv"
        original_file_dir_path = os.path.dirname(original_file_path)
        new_file_full_path = os.path.join(original_file_dir_path, file_name)

        os.makedirs(os.path.dirname(new_file_full_path), exist_ok=True)
        df.to_csv(new_file_full_path, sep='\t', index=False)

    def run(self):
        input_files = [file.path for file in self.input()[1]]
        for file in input_files:
            log.info(f"[INFO] Spliting the file {file} into tables is in progress ...")
            self.to_tables(file)

@requires(SeparateTables)
class TableProcessing(luigi.Task):
    def output(self):
        if self.input():
            input_files = [file.path for file in self.input()]
            input_dirs = set([os.path.dirname(file) for file in input_files])
            return [luigi.LocalTarget(os.path.join(dir, 'Probes_cleaned.tsv')) for dir in input_dirs]

    # Шаг 3:
        #   Из него нужно убрать следующие колонки: Definition, Ontology_Component, Ontology_Process,
        #   Ontology_Function, Synonyms, Obsolete_Probe_Idz, Probe_Sequence.                
    def clean_table_columns(self, file_path):
        df = pd.read_csv(file_path, sep='\t')
        df = df.drop(columns=['Definition',
                              'Ontology_Component',
                              'Ontology_Process',
                              'Ontology_Function',
                              'Synonyms',
                              'Obsolete_Probe_Id',
                              'Probe_Sequence'])
        new_file_path = str.replace(file_path, 'Probes', 'Probes_cleaned')
        df.to_csv(new_file_path, sep='\t', index=False)

    def run(self):
        input_files = [file.path for file in self.input()]
        for file in input_files:
            if os.path.basename(file) == 'Probes.tsv':
                log.info(f"[INFO] Cleaning Probes dataframe is in progress ...")
                self.clean_table_columns(file)

@requires(TableProcessing)
class ClearEnviroment(luigi.Task):    
    def run(self):
        input_files = [file.path for file in self.input()]
        directories = [os.path.dirname(file) for file in input_files]

        for dir in directories:
            for file in os.listdir(dir):
                if not file.endswith('.tsv'):
                    log.info(f"[INFO] Deleting {file} is in progress ...")     
                    file_path = os.path.join(dir, file)
                    os.remove(file_path)     

if __name__ == '__main__':
    luigi.run()
