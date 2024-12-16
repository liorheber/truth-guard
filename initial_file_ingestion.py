import os

from PyPDF2 import PdfReader, PdfWriter
from src.database import create_snowflake_session, init_database

session = create_snowflake_session()
init_database(session)

chunk_page_size = 200
documents_dir_path = os.path.join(os.path.dirname(__file__), "documents")
split_files_dir_path = os.path.join(os.path.dirname(__file__), "tmp", "split_files")

if not os.path.exists(split_files_dir_path):
    os.makedirs(split_files_dir_path)

for file_name in os.listdir(documents_dir_path):
    print(f"starting to process {file_name}")
    file_path = os.path.join(documents_dir_path, file_name)
    split_file_path = os.path.join(split_files_dir_path, file_name)

    reader = PdfReader(file_path)

    pages = (0, chunk_page_size)

    pages_in_file = len(reader.pages)

    current_last_page = min(pages_in_file, chunk_page_size)
    split_file_names = []
    while pages[0] < pages_in_file:
        page_range = range(pages[0], pages[1])
        writer = PdfWriter()

        for page_num, page in enumerate(reader.pages, 1):
            if page_num in page_range:
                writer.add_page(page)

        split_file_name = f'{split_file_path.replace(" ", "_")}_page_{pages[0]}-{pages[1]}.pdf'

        with open(split_file_name, 'wb') as out:
            writer.write(out)

        print(f"uploading {split_file_name} to @DOCUMENT_STAGE")

        session.sql(f"PUT file://{split_file_name} @DOCUMENT_STAGE").collect()

        print(f"{split_file_name} uploaded")
        pages = (pages[0] + chunk_page_size, pages[1] + chunk_page_size)
