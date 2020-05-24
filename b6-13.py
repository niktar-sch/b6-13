import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from bottle import request
from bottle import route
from bottle import run
from bottle import HTTPError



DB_PATH = "sqlite:///albums.sqlite3"
Base = declarative_base()


class Album(Base):
    """
    Описывает структуру таблицы album для хранения записей музыкальной библиотеки
    """

    __tablename__ = "album"

    id = sa.Column(sa.INTEGER, primary_key=True)
    year = sa.Column(sa.INTEGER)
    artist = sa.Column(sa.TEXT)
    genre = sa.Column(sa.TEXT)
    album = sa.Column(sa.TEXT)


def connect_db():
    """
    Устанавливает соединение к базе данных, создает таблицы, если их еще нет и возвращает объект сессии 
    """
    engine = sa.create_engine(DB_PATH, echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker(engine)
    return session()


@route("/albums/<artist>", methos="GET")
def albums(artist):
    """
    Находит все альбомы в базе данных по заданному артисту
    """
    session = connect_db()
    albums_list = session.query(Album).filter(Album.artist == artist).all()
    if albums_list:
        album_names = [album.album for album in albums_list]
        result = "Колчество альбомов {}: {}, список: {}".format(artist, len(album_names), ", ".join(album_names))
    else:
        result = "Альбомов {} не найдено".format(artist)
    return result

@route("/albums", method="POST")
def album_add():
    """
    Добавляет альбом в базу данных
    """
    #Проверка наличия всех полей
    not_found_fields = {"year", "artist", "genre", "album"} - request.forms.keys()
    if not_found_fields:
        message = "Переданы не все поля. Отсутствуют: {}".format(', '.join(not_found_fields))
        return HTTPError(400, message)
    try:
        year = int(request.forms.get("year"))
        if not 1870 <= year <= 3000:
            raise ValueError()
        album = Album(
            year=year,
            artist=request.forms.get("artist"),
            genre=request.forms.get("genre"),
            album=request.forms.get("album")
        )
    except ValueError:
        message = "Неверный год: \"{}\"".format(request.forms.get("year"))
        return HTTPError(409, message)
        
    session = connect_db()
    album_rec = session.query(Album)\
                .filter(sa.and_(Album.year == year,
                                Album.artist == album.artist,
                                Album.genre == album.genre,
                                Album.album == album.album))\
                .first()
    if album_rec:
        message = "Альбом {} группы {} в жанре {} {} года уже добавлен".format(album.album, album.artist, album.genre, album.year)
        return HTTPError(409, message)
    else:
        session.add(album)
        session.commit()
        return "Данные успешно сохранены"

if __name__ == "__main__":
    run(host="localhost", port=8080, debug=True)
