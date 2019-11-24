from sqlalchemy import Table, Column, ForeignKey, Integer, String, Boolean, DateTime, PrimaryKeyConstraint, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

BaseCardax = declarative_base()
BaseDatabank = declarative_base()

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
    card_number = Column(String(80), nullable=False, index=True)
    status = Column(String(20))
    card_type = Column(String(50))
    cardholder_id = Column(Integer, ForeignKey('cardholder.id'))
    cardholder = relationship("Cardholder", back_populates="cards")


class CardOneID(BaseDatabank):
    __tablename__ = "unicard_card_oneid"
    id = Column(Integer, Sequence('unicard_card_oneid_id_seq'), primary_key=True)
    intserial = Column(Integer, ForeignKey("unicard_card.intserial"))
    one_id = Column(String(50), index=True)
    card = relationship("UnicardCard", back_populates="one_ids")


class AccessZone(BaseCardax):
    __tablename__ = "access_zone"
    id = Column(Integer, primary_key=True)
    name = Column(String(250), index=True)


class Door(BaseCardax):
    __tablename__ = "door"
    id = Column(Integer, primary_key=True)
    name = Column(String(250), index=True)


class EventGroup(BaseCardax):
    __tablename__ = "event_group"
    id = Column(Integer, primary_key=True)
    name = Column(String(250), index=True)
    event_types = relationship("EventType", back_populates="event_group")


class EventType(BaseCardax):
    __tablename__ = "event_type"
    id = Column(Integer, primary_key=True)
    name = Column(String(250), index=True)
    event_group_id = Column(Integer, ForeignKey("event_group.id"))
    event_group = relationship("EventGroup", back_populates="event_types")


class Event(BaseCardax):
    __tablename__ = "event"
    id = Column(Integer, primary_key=True)
    event_time = Column(DateTime, nullable=False)
    card_number = Column(String(80), nullable=False, index=True)
    card_facility_code = Column(String(30))
    cardholder_id = Column(Integer, nullable=False, index=True)
    entry_access_zone = Column(Integer, index=True)
    exit_access_zone = Column(Integer, index=True)
    door_id = Column(Integer, nullable=False)
    event_type_id = Column(Integer, ForeignKey("event_type.id"))
    event_type = relationship("EventType")


class Patron(BaseDatabank):
    __tablename__ = "databank_patron"
    one_id = Column(String(20), primary_key=True)
    party_id = Column(Integer, index=True)
    first_name = Column(String(250))
    last_name = Column(String(250))
    source_system = Column(String(10))
    faculty = Column(String(50), index=True)
    category = Column(String(50), index=True)


class UnicardCard(BaseDatabank):
    __tablename__ = "unicard_card"
    intserial = Column(Integer, primary_key=True)
    party_id = Column(Integer, index=True)
    barcode = Column(String(50))
    ac_num = Column(Integer, index=True)
    card_type = Column(String(50))
    first_name = Column(String(250))
    last_name = Column(String(250))
    print_reason = Column(String(250))
    one_ids = relationship("CardOneID", back_populates="card")

class Faculty(BaseDatabank):
    __tablename__ = "faculty"
    one_id = Column(String(20), primary_key=True)
    faculty_name = Column(String(250))
