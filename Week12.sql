--q3
USE DESTINATION8271;

DROP TABLE IF EXISTS ACTOR;

CREATE TABLE ACTOR (
    ACTORID INT PRIMARY KEY,
    FIRSTNAME VARCHAR(100),
    SURNAME VARCHAR(100),
    GENDER VARCHAR(1),
    DATEOFBIRTH DATE,
    BIRTHCOUNTRY VARCHAR(50)

);

--q4
DROP TABLE IF EXISTS ERROR_EVENT8271;
CREATE TABLE ERROR_EVENT8271 (
    ERRORID INT IDENTITY(1,1) PRIMARY KEY,
    SOURCEDB NVARCHAR(100) NOT NULL,
    SOURCETABLE NVARCHAR(100) NOT NULL,
    SOURCEPKCOL NVARCHAR(100) NOT NULL,
    SOURCEPK NVARCHAR(100) NOT NULL,
    FILTERID INT NOT NULL,
    ACTION NVARCHAR(100) NOT NULL,
);

CREATE TABLE COUNTRY_SPELLING8271 (
    INVALID NVARCHAR(100),
    VALID NVARCHAR(100) NOT NULL,
);

INSERT INTO COUNTRY_SPELLING8271 (INVALID,VALID)
VALUES  ('UAS','USA'),
('United States','USA'),
('United States of America','USA'),
('Unyted Staytes','USA'),
('United Staytes','USA'),
('Aussie','Australia'),
('Straya','Australia'),
('OZ','Australia'),
('Down Under','Australia'),
('Kiwi Land','New Zealand');

--q5
SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor');

--q6
USE DESTINATION8271;

SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE BIRTHCOUNTRY IN (SELECT CS.INVALID FROM COUNTRY_SPELLING8271 CS WHERE BIRTHCOUNTRY = INVALID);


--q7
INSERT INTO ERROR_EVENT8271 (
    SOURCEDB,
    SOURCETABLE,
    SOURCEPKCOL,
    SOURCEPK,
    FILTERID,
    [ACTION]
)
SELECT 
    'Source',
    'Actor',
    'ActorNo',
    ACTORNO,
    1,
    'MODIFY'
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;','SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE BIRTHCOUNTRY IN (SELECT INVALID
                        FROM COUNTRY_SPELLING8271
                        WHERE BIRTHCOUNTRY = INVALID)

SELECT * FROM ERROR_EVENT8271


--q8
SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE BIRTHCOUNTRY IN (SELECT CS.INVALID FROM COUNTRY_SPELLING8271 CS 
WHERE BIRTHCOUNTRY = NULL OR BIRTHCOUNTRY = INVALID OR BIRTHCOUNTRY = VALID);

--q9
INSERT INTO ERROR_EVENT8271 (
    SOURCEDB,
    SOURCETABLE,
    SOURCEPKCOL,
    SOURCEPK,
    FILTERID,
    [ACTION]
)
SELECT 
    'Source',
    'Actor',
    'ActorNo',
    ACTORNO,
    1,
    'MODIFY'
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;','SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE BIRTHCOUNTRY IN (SELECT INVALID
                        FROM COUNTRY_SPELLING8271
                        WHERE BIRTHCOUNTRY = NULL OR 
                        BIRTHCOUNTRY = INVALID OR BIRTHCOUNTRY = VALID)

SELECT * FROM ERROR_EVENT8271

--q10
SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE ACTORNO NOT IN (SELECT SOURCEPK FROM ERROR_EVENT8271);

--q11
INSERT INTO ACTOR( ACTORID, FIRSTNAME, SURNAME, GENDER, DATEOFBIRTH, BIRTHCOUNTRY)
SELECT ACTORNO, GIVENNAME, SURNAME, GENDER, BIRTHDATE, BIRTHCOUNTRY FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE ACTORNO NOT IN (SELECT SOURCEPK FROM ERROR_EVENT8271);

SELECT * FROM ACTOR;

--q12

SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE ACTORNO IN (SELECT SOURCEPK
                    FROM ERROR_EVENT
                    WHERE FILTERID = 1)

UPDATE OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
SET BIRTHCOUNTRY = (SELECT VALID
                    FROM COUNTRY_SPELLING
                    WHERE BIRTHCOUNTRY = INVALID)
WHERE BIRTHCOUNTRY IN (SELECT INVALID
                        FROM COUNTRY_SPELLING)

SELECT *
FROM OPENROWSET('SQLNCLI', 'Server=ddc.cmetqswvqooi.us-west-1.rds.amazonaws.com;UID=admin;PWD=Dayezobel*70;',
'SELECT * FROM ETL_SOURCE.dbo.actor')
WHERE ACTORNO IN (SELECT SOURCEPK
                    FROM ERROR_EVENT
                    WHERE FILTERID = 1)