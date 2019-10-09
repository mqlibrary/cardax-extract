from sqlalchemy import Table, Column, ForeignKey, Integer, String, Boolean, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

BaseDatabank = declarative_base()


class Patron(BaseDatabank):
    __tablename__ = "databank_patron"
    one_id = Column(String(20), primary_key=True)
    party_id = Column(Integer, index=True)
    first_name = Column(String(250))
    last_name = Column(String(250))
    source_system = Column(String(10))


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


class CardOneID(BaseDatabank):
    __tablename__ = "unicard_card_oneid"
    id = Column(Integer, primary_key=True)
    intserial = Column(Integer, ForeignKey("unicard_card.intserial"))
    one_id = Column(String(50), index=True)
    card = relationship("Card", back_populates="one_ids")
