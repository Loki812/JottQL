to build a database run 
java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>
dbLocation -  (String) The path to the database or where the database should be created. can be ablsolute or reletive  
pageSize   -  (integer) The amount of bytes a page takes up 
bufferSize -  (integer) The number of pages the buffer can hold 
indexing   -  (boolean) Weather the database utilizes B+ tree indexing or not

Once database is running you can use the following comands 

CREATE TABLE <table name> (<atributes>);

DROP TABLE <table name>;

ALTER TABLE <table name> <ADD/DROP> atribute;

SELECT <atributes> FROM <tables> WHERE <equality>;

INSERT <table name> VALUES (<values>);

DELEATE

UPDATE 
