DROP STREAM IF EXISTS appointment_events;
CREATE STREAM appointment_events
(
    _appointment_id uint32,
    _id string,
    _status string,
    _set_by_id string,
    _company_id string,
    _client_id string,
    _type string,
    _at string,
    _vacancy_id string,
    _set_at uint32,
    _job_requisition_id string
) ENGINE = Memory;

INSERT INTO appointment_events (_appointment_id, _set_at, _status) values (1, 1, 'Created'), (2, 2, 'Created');

SELECT A._appointment_id,
       A._id,
       A._status,
       A._set_by_id,
       A._company_id,
       A._client_id,
       A._type,
       A._at,
       A._vacancy_id,
       A._set_at,
       A._job_requisition_id
FROM appointment_events as A ANY
LEFT JOIN
  (SELECT _appointment_id,
          max(_set_at) AS max_set_at
   FROM appointment_events
   WHERE _status in ('Created', 'Transferred')
   GROUP BY _appointment_id ) as B USING _appointment_id
WHERE A._set_at = B.max_set_at;

DROP STREAM appointment_events;
