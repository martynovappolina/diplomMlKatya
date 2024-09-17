import json
import uuid
from datetime import datetime
from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from Domain.aiReportResult import AiReportResult
from utils import get_db
from ..router import router
from langchain.chat_models.gigachat import GigaChat
from langchain.schema import HumanMessage

@router.post("/createRequest")
def create_request(command: str, db: Session = Depends(get_db)):
    origin_command = command
    sql = ''
    result = ''
    is_error = False
    id = str(uuid.uuid4())

    try:
        command = 'Есть вот такие таблицы в postgresql:\n' + \
                  '- public."authors" - Авторы\n' + \
                  '    - "id" - Идентификатор\n' + \
                  '    - "name" - Имя автора\n' + \
                  '\n' + \
                  '- public."books" - Книги\n' + \
                  '    - "id" - Идентификатор\n' + \
                  '    - "title" - Название книги\n' + \
                  '    - "author_id" - Идентификатор автора (ссылка на authors)\n' + \
                  '\n' + \
                  '- public."borrowers" - Читатели\n' + \
                  '    - "id" - Идентификатор\n' + \
                  '    - "name" - Имя читателя\n' + \
                  '    - "email" - Email читателя (уникальный)\n' + \
                  '\n' + \
                  'Напиши sql запрос для такого текстового запроса:\n' + \
                  command + '\n';

        chat = GigaChat(
            credentials='OTc1NGNmZjQtOTAxNi00OWU4LWJmZjgtMDIzZmUzNTY3ZTg4OjI3NTZmZmU2LTJiZGMtNDlhZi05ZmM2LTdiMWNlZmZlOTdkMw==',
            verify_ssl_certs=False
        )

        messages = [
            HumanMessage(
                content=command
            )
        ]

        sql = chat(messages).content.split('```')[1].replace('sql', '')
        fields = ['id', 'name', 'title', 'author_id', 'email']

        for field in fields:
            sql = sql.replace(f'.{field} ', f'."{field}"')
            sql = sql.replace(f'.{field};', f'."{field}";')
            sql = sql.replace(f'.{field},', f'."{field}",')
            sql = sql.replace(f' {field},', f' "{field}",')
            sql = sql.replace(f' {field};', f' "{field}";')
            sql = sql.replace(f'.{field})', f'."{field}")')
            sql = sql.replace(f'.{field}\n', f'."{field}"\n')
            if sql.endswith(f'.{field}'):
                sql = sql.replace(f'.{field}', f'."{field}"')

        tables = ['authors', 'books', 'borrowers']
        for table in tables:
            sql = sql.replace(f'FROM {table}', f'FROM public."{table}"')
            sql = sql.replace(f'JOIN {table}', f'JOIN public."{table}"')

        rows = db.execute(text(sql)).all()
        headers = db.execute(text(sql)).keys()
        headers = [header for header in headers]
        rows = [tuple(row) for row in rows]
        if len(rows) > 500:
            raise Exception('too many rows')
        result = json.dumps({"error": False, "headers": headers, "rows": rows, "id": str(id)})
    except Exception as e:
        print(e)
        is_error = True
        result = json.dumps({"error": True})
    finally:
        if is_error:
            db = next(get_db())
        db.add(AiReportResult(
            Id=id,
            CreateAt=datetime.now(),
            Request=origin_command,
            IsSuccess=not is_error,
            Sql=sql,
            Result=result
        ))
        db.commit()
        return rows[0][0]
