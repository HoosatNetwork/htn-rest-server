from sqlalchemy import Column, String

from dbsession import Base

class AddressTag(Base):
    __tablename__ = 'address_tags'

    address = Column(String, primary_key=True)
    label = Column(String, nullable=False)

    def __init__(self, address, label):
        self.address = address
        self.label = label
