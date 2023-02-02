SELECT * FROM VALUES('x uint8, y uint16', 1 + 2, 'Hello'); -- { serverError 36 }
