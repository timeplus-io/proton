-- -------------------------------------------------------------------------------------------------
-- Insert into kafka from nexmark data generator.
-- -------------------------------------------------------------------------------------------------

INSERT INTO kafka
SELECT event_type, person, auction, bid FROM datagen;