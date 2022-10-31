create stream if not exists auction (
    id string,
    initialBid string,
    itemName string,
    reserve string,
    seller string,
    category string,
    dateTime datetime64(3),
    description string,
    expires datetime64(3),
    extra string
);
create stream if not exists person(
    id string,
    name string,
    state string,
    city string,
    creditCard string,
    dateTime datetime64(3),
    emailAddress string,
    extra string
);
create stream if not exists bid (
    auction string,
    bidder string,
    channel string,
    price float,
    url string,
    dateTime datetime64(3),
    extra string
)