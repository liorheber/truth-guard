import os

from PyPDF2 import PdfReader, PdfWriter
from src.database import *

chunk_page_size = 200
documents_dir_path = os.path.join(os.path.dirname(__file__), "documents")
split_files_dir_path = os.path.join(os.path.dirname(__file__), "tmp", "split_files")


def init_connection_and_db():
    session = create_snowflake_session()
    if init_database(session):
        return session
    else:
        raise Exception("Error: Unable to access required Cortex functions")


def write_file_to_stage(session, file: str, stage: str):
    print(f"uploading {file} to @{stage}")
    res = session.sql(f"PUT file://{file} @{stage} auto_compress=FALSE OVERWRITE=TRUE").collect()
    if not all([r["status"] == "UPLOADED" for r in res]):
        raise Exception(f"Error: Unable to upload {file} to @{stage}")
    print(f"{file} uploaded")
    print(res)
    return res


def write_page_range_to_stage(session, reader: PdfReader, split_file_name: str, stage: str, page_range: range):
    writer = PdfWriter()
    for page_num, page in enumerate(reader.pages, 1):
        if page_num in page_range:
            writer.add_page(page)
    with open(split_file_name, 'wb') as out:
        writer.write(out)
    res = write_file_to_stage(session, split_file_name, stage)
    print(res)
    os.remove(split_file_name)
    return True


def chunk_and_upload_file(session, file: str, stage: str, chunk_size: int):
    reader = PdfReader(file)
    pages = (0, chunk_size)
    pages_in_file = len(reader.pages)
    split_file_path = os.path.join(split_files_dir_path, os.path.basename(file)).replace(" ", "_")
    uploaded_files = []
    while pages[0] < pages_in_file:
        split_file_name = f'{split_file_path.replace(".pdf", "")}_page_{pages[0]}-{pages[1]}.pdf'
        page_range = range(pages[0], pages[1])
        write_page_range_to_stage(session, reader, split_file_name, stage, page_range)
        uploaded_files.append(split_file_name)
        pages = (pages[1], pages[1] + chunk_size)
    return uploaded_files


def upload_file_to_stage(session, file: str, stage: str):
    print(f"start processing {file}")
    uploaded_files = chunk_and_upload_file(session, file, stage, chunk_page_size)
    print(f"{file} processed")
    return uploaded_files


def chunks_into_table(session, stage: str, table: str):
    print(f"inserting chunks from {stage}")
    chunking_sql = (f"insert into {table}"
                    f" (relative_path, size, file_url, scoped_file_url, chunk) "
                    f"select relative_path, size, file_url, "
                    f"build_scoped_file_url(@{stage}, relative_path) as scoped_file_url,  "
                    f"func.chunk as chunk "
                    f"from directory(@{stage}),  "
                    f"TABLE("
                    f"text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@{stage},  "
                    f"relative_path, ") + "{'mode': 'LAYOUT'})))) as func;"
    print(chunking_sql)
    results = session.sql(chunking_sql).collect()
    print(results)
    if results[0][0] == 0:
        print("No chunks inserted")
        return False
    print("chunks inserted")
    return True


if __name__ == "__main__":
    cur_session = init_connection_and_db()
    if not os.path.exists(split_files_dir_path):
        os.makedirs(split_files_dir_path)

    for file_name in os.listdir(documents_dir_path):
        print(f"starting to process {file_name}")
        file_path = os.path.join(documents_dir_path, file_name)
        upload_file_to_stage(cur_session, file_path, VERIFIED_DOCUMENT_STAGE)

    chunks_into_table(cur_session, VERIFIED_DOCUMENT_STAGE, VERIFIED_DOCS_CHUNKS)
