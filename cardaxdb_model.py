from sqlalchemy import Table, Column, ForeignKey, Integer, String, Boolean, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

BaseCardax = declarative_base()

cardholder_access_group = Table('cardholder_access_group', BaseCardax.metadata,
                                Column('cardholder_id', Integer, ForeignKey('cardholder.id')),
                                Column('access_group_id', Integer, ForeignKey('access_group.id'))
                                )


class Cardholder(BaseCardax):
    __tablename__ = 'cardholder'
    id = Column(Integer, primary_key=True)
    unique_id = Column(String(50), index=True)
    one_id = Column(String(50), index=True)
    party_id = Column(Integer, index=True)
    db_party_id = Column(Integer, index=True)
    authorised = Column(Boolean, nullable=False)
    division = Column(Integer, nullable=False)
    first_name = Column(String(250))
    last_name = Column(String(250))
    description = Column(String(250))
    last_successful_access_time = Column(DateTime)
    last_successful_access_zone = Column(String(250))
    access_groups = relationship("AccessGroup", back_populates="cardholder", secondary="cardholder_access_group")
    cards = relationship("Card", back_populates="cardholder")


class AccessGroup(BaseCardax):
    __tablename__ = 'access_group'
    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    cardholder = relationship("Cardholder", back_populates="access_groups", secondary="cardholder_access_group")


class Card(BaseCardax):
    __tablename__ = 'card'
    id = Column(String(250), primary_key=True)
    issue_level = Column(Integer, nullable=False)
    number = Column(String(80), nullable=False, index=True)
    status = Column(String(20))
    card_type = Column(String(50))
    cardholder_id = Column(Integer, ForeignKey('cardholder.id'))
    cardholder = relationship("Cardholder", back_populates="cards")
