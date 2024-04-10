import json
import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import inspect

Base = declarative_base()



class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)


    publisher = relationship(Publisher, backref='book')
   

class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)
    

class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)


    book = relationship(Book, backref='stock')
    shop = relationship(Shop, backref='stock')


class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)


    stock = relationship(Stock, backref='sale')


def create_tables(engine):
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    if existing_tables:
        print('Tables already exist:', existing_tables)
        return

    try:
        Base.metadata.create_all(engine)
        print('Tables created successfully')
    except sqlalchemy.exc.OperationalError as e:
        print('Error: ', e)


def insert_data(session, data):
    for item in data:
        if item['model'] == 'publisher':
            publisher = Publisher(id=item['pk'], name=item['fields']['name'])
            session.add(publisher)
        elif item['model'] == 'book':
            book = Book(id=item['pk'], title=item['fields']['title'], id_publisher=item['fields']['id_publisher'])
            session.add(book)
        elif item['model'] == 'shop':
            shop = Shop(id=item['pk'], name=item['fields']['name'])
            session.add(shop)
        elif item['model'] == 'stock':
            stock = Stock(id=item['pk'], id_shop=item['fields']['id_shop'], id_book=item['fields']['id_book'], count=item['fields']['count'])
            session.add(stock)
        elif item['model'] == 'sale':
            sale = Sale(id=item['pk'], price=float(item['fields']['price']), date_sale=item['fields']['date_sale'], count=item['fields']['count'], id_stock=item['fields']['id_stock'])
            session.add(sale)


def query_data(session, publisher_id):
    if isinstance(publisher_id, int):
        publisher = session.query(Publisher).filter_by(id=publisher_id).first()
    else:
        publisher = session.query(Publisher).filter_by(name=publisher_id).first()

    if not publisher:
        print('Издатель с указанным идентификатором или именем не найден.')
        return

    sales = session.query(Sale)\
                  .join(Stock)\
                  .join(Book)\
                  .filter(Book.id_publisher == publisher.id)\
                  .all()

    if not sales:
        print('Для этого издателя нет записей о продажах.')
        return

    for sale in sales:
        print(f"{sale.stock.book.title} | {sale.stock.shop.name} | {sale.price} | {sale.date_sale}")


if __name__ == "__main__":
    DSN = 'postgresql://postgres:kisharra@localhost:5432/book_db'
    engine = sqlalchemy.create_engine(DSN)
    # create_tables(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    with open('insert_data.json', 'r') as f:
        data = json.load(f)
    
    # insert_data(session, data)

    publisher_input = input('Введите ID или название издателя: ')
    try:
        publisher_id = int(publisher_input)
        query_data(session, publisher_id)
    except ValueError:
        query_data(session, publisher_input)

    session.commit()






