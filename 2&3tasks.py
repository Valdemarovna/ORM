import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale

# Создаем подключение к бд
DSN = "postgresql://postgres:postgres@localhost:5432/ORM_tasks"
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)

#Записываем данные в бд
with open('test_data.json', 'rt') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def books_sale(issuer_id=input('Укажите имя или идентификатор издателя: ')):
    issuer_id = issuer_id
    if issuer_id.isnumeric():
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.id == issuer_id).all()
        for book, shop, price, date in results:
            print(f'{book: <40} | {shop: <10} | {price: <10} | {date}')
    else:
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.name == issuer_id).all()
        for book, shop, price, date in results:
            print(f'{book: <40} | {shop: <10} | {price: <10} | {date}')

session.close()
books_sale()
