DROP STREAM IF EXISTS null_issue_3767;

CREATE STREAM null_issue_3767 (value nullable(string)) ENGINE=Memory;

INSERT INTO null_issue_3767 (value) VALUES ('A string'), (NULL);

SELECT value FROM null_issue_3767 WHERE value NOT IN ('A string');

DROP STREAM null_issue_3767;
