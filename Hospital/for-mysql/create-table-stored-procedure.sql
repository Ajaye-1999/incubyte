-- date cannot have length
-- doctorConsulted, country, etc.. cannot be char, so I have considered it as varchar

DROP PROCEDURE incubyte.createCountryTable;
CREATE PROCEDURE incubyte.createCountryTable(
IN CountryName VARCHAR(40)
)
BEGIN
SET @table := CountryName;
SET @sql_text:=CONCAT('
CREATE TABLE incubyte.',@table, '(
customerName varchar(255) not null,
customerID bigint(18) auto_increment,
customerOpenDate date not null,
lastConsultedDate date,
vaccinationType varchar(5),
doctorConsulted varchar(255),
state varchar(5),
country varchar(5),
postCode integer(5),
dateOfBirth date,
age integer(3),
last_consulted_days integer(3),
activeCustomer char(1),
primary key(customerID)
)');
PREPARE stmt from @sql_text;
EXECUTE stmt;
END;


-- TO create table use:
-- call createCountryTable("tblName");


-- Select all lines in `dbeaver` and then hit ctrl enter
-- else it will throw Error 1064


-- must be some issue with terminal, this is working in dbeaver
-- but not in Ubuntu Terminal
