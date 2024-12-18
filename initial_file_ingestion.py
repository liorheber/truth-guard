import os

from PyPDF2 import PdfReader, PdfWriter
from src.database import *

session = create_snowflake_session()
init_database(session)

chunk_page_size = 200
documents_dir_path = os.path.join(os.path.dirname(__file__), "documents")
split_files_dir_path = os.path.join(os.path.dirname(__file__), "tmp", "split_files")


def write_file_to_stage(file: str, stage: str):
    print(f"uploading {file} to @{stage}")
    session.sql(f"PUT file://{file} @{stage}").collect()
    print(f"{file} uploaded")


def write_page_range_to_stage(reader: PdfReader, split_file_name: str, stage: str, page_range: range):
    writer = PdfWriter()
    for page_num, page in enumerate(reader.pages, 1):
        if page_num in page_range:
            writer.add_page(page)
    with open(split_file_name, 'wb') as out:
        writer.write(out)
    write_file_to_stage(split_file_name, stage)


def chunk_and_upload_file(file: str, stage: str, chunk_size: int):
    reader = PdfReader(file)
    pages = (0, chunk_size)
    pages_in_file = len(reader.pages)
    split_file_path = os.path.join(split_files_dir_path, os.path.basename(file)).replace(" ", "_")
    while pages[0] < pages_in_file:
        split_file_name = f'{split_file_path}_page_{pages[0]}-{pages[1]}.pdf'
        page_range = range(pages[0], pages[1])
        write_page_range_to_stage(reader, split_file_name, stage, page_range)
        pages = (pages[1], pages[1] + chunk_size)


def upload_file_to_stage(file: str, stage: str):
    print(f"start processing {file}")
    chunk_and_upload_file(file, stage, chunk_page_size)
    print(f"{file} processed")


def chunks_into_table(stage: str):
    session.sql(
        f"insert into {stage} "
        f"(relative_path, size, file_url, scoped_file_url, chunk) "
        f"select relative_path, size, file_url, "
        f"build_scoped_file_url(@{stage}, relative_path) as scoped_file_url, "
        f"func.chunk as chunk from directory(@{stage}), "
        f"TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@{stage}, "
        f"relative_path, " +
        "{'mode': 'LAYOUT'})))) as func;"
    ).collect()


if __name__ == "__main__":
    if not os.path.exists(split_files_dir_path):
        os.makedirs(split_files_dir_path)

    for file_name in os.listdir(documents_dir_path):
        print(f"starting to process {file_name}")
        file_path = os.path.join(documents_dir_path, file_name)
        upload_file_to_stage(file_path, VERIFIED_DOCUMENT_STAGE)

    chunks_into_table(VERIFIED_DOCUMENT_STAGE)
