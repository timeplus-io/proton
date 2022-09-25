CREATE VIEW person AS
SELECT
    person.id,
    person.name,
    person.emailAddress,
    person.creditCard,
    person.city,
    person.state,
    dateTime,
    person.extra
FROM ${NEXMARK_TABLE} WHERE event_type = 0;

CREATE VIEW auction AS
SELECT
    auction.id,
    auction.itemName,
    auction.description,
    auction.initialBid,
    auction.reserve,
    dateTime,
    auction.expires,
    auction.seller,
    auction.category,
    auction.extra
FROM ${NEXMARK_TABLE} WHERE event_type = 1;

CREATE VIEW bid AS
SELECT
    bid.auction,
    bid.bidder,
    bid.price,
    bid.channel,
    bid.url,
    dateTime,
    bid.extra
FROM ${NEXMARK_TABLE} WHERE event_type = 2;