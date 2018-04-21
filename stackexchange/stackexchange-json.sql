/* ----------------------------------------------------------------------
 *
 * This script assumes you have loaded the base data by running the Base.sql file.
 *
 * mysql < Json.sql
 */

USE StackExchange;

DROP TABLE IF EXISTS StackExchange.VotesJson;
DROP TABLE IF EXISTS StackExchange.PostHistoryJson;
DROP TABLE IF EXISTS StackExchange.CommentsJson;
DROP TABLE IF EXISTS StackExchange.PostsJson;
DROP TABLE IF EXISTS StackExchange.BadgesJson;
DROP TABLE IF EXISTS StackExchange.UsersJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE UsersJson (
  Id               INT AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO UsersJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'Reputation', Reputation,
  'CreationDate', CreationDate,
  'DisplayName', DisplayName,
  'LastAccessDate', LastAccessDate,
  'WebsiteUrl', WebsiteUrl,
  'Location', Location,
  'Age', Age,
  'AboutMe', AboutMe,
  'Views', Views,
  'UpVotes', UpVotes,
  'DownVotes', DownVotes
) FROM Users;

ANALYZE TABLE UsersJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE BadgesJson (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO BadgesJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'BadgeTypeId', BadgeTypeId,
  'UserId', CreationDate,
  'CreationDate', CreationDate
) FROM Badges;

ALTER TABLE BadgesJson
  ADD COLUMN BadgeTypeId SMALLINT UNSIGNED AS (Data->'$.BadgeTypeId') STORED,
  ADD FOREIGN KEY (BadgeTypeId) REFERENCES StackExchange.BadgeTypes(Id);

ALTER TABLE BadgesJson
  ADD COLUMN UserId INT AS (Data->'$.Userid') STORED,
  ADD FOREIGN KEY (UserId) REFERENCES StackExchange.Users(Id);

ANALYZE TABLE BadgesJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE PostsJson (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO PostsJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'PostTypeId', PostTypeId,
  'AcceptedAnswerId', AcceptedAnswerId,
  'ParentId', ParentId,
  'CreationDate', CreationDate,
  'Score', Score,
  'ViewCount', ViewCount,
  'Body', Body,
  'OwnerUserId', OwnerUserId,
  'LastEditorUserId', LastEditorUserId,
  'LastEditDate', LastEditDate,
  'LastActivityDate', LastActivityDate,
  'Title', Title,
  'Tags', Tags,
  'AnswerCount', AnswerCount,
  'CommentCount', CommentCount,
  'FavoriteCount', FavoriteCount,
  'ClosedDate', ClosedDate
) FROM Posts;

ALTER TABLE PostsJson
  ADD COLUMN PostTypeId TINYINT UNSIGNED AS (Data->>'$.PostTypeId') STORED,
  ADD FOREIGN KEY (PostTypeId) REFERENCES PostTypes(Id);

ALTER TABLE PostsJson
  ADD COLUMN AcceptedAnswerId INT UNSIGNED AS (NULLIF(Data->>'$.AcceptedAnswerId', 'null')) STORED,
  ADD FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id);

ALTER TABLE PostsJson
  ADD COLUMN ParentId INT UNSIGNED AS (NULLIF(Data->>'$.ParentId', 'null')) STORED,
  ADD FOREIGN KEY (ParentId) REFERENCES Posts(Id);

ALTER TABLE PostsJson
  ADD COLUMN OwnerUserId INT AS (NULLIF(Data->>'$.OwnerUserId', 'null')) STORED,
  ADD FOREIGN KEY (OwnerUserId) REFERENCES Users(Id);

ALTER TABLE PostsJson
  ADD COLUMN LastEditorUserId INT AS (NULLIF(Data->>'$.LastEditorUserId', 'null')) STORED,
  ADD FOREIGN KEY (LastEditorUserId) REFERENCES Users(Id);

ANALYZE TABLE PostsJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE CommentsJson (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO CommentsJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'PostId', PostId,
  'Score', Score,
  'Text', Text,
  'CreationDate', CreationDate,
  'UserId', UserId
) FROM Comments;

ALTER TABLE CommentsJson
  ADD COLUMN PostId INT UNSIGNED AS (Data->'$.PostId') STORED,
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id);

ALTER TABLE CommentsJson
  ADD COLUMN UserId INT AS (Data->'$.UserId') STORED,
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ANALYZE TABLE CommentsJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE PostHistoryJson (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO PostHistoryJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'PostHistoryTypeId', PostHistoryTypeId,
  'PostId', PostId,
  'RevisionGUID', RevisionGUID,
  'CreationDate', CreationDate,
  'UserId', UserId,
  'Text', Text
) FROM PostHistory;

ALTER TABLE PostHistoryJson
  ADD COLUMN PostHistoryTypeId TINYINT UNSIGNED AS (Data->'$.PostHistoryTypeId') STORED,
  ADD FOREIGN KEY (PostHistoryTypeId) REFERENCES PostHistoryTypes(Id);

ALTER TABLE PostHistoryJson
  ADD COLUMN PostId INT UNSIGNED AS (Data->'$.PostId') STORED,
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id);

ALTER TABLE PostHistoryJson
  ADD COLUMN UserId INT AS (NULLIF(Data->>'$.UserId', 'null')) STORED,
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ANALYZE TABLE PostHistoryJson;

/* ---------------------------------------------------------------------- */

CREATE TABLE VotesJson (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Data             JSON NOT NULL
);

INSERT INTO VotesJson (Id, Data)
SELECT Id, JSON_OBJECT(
  'PostId', PostId,
  'VoteTypeId', VoteTypeId,
  'UserId', UserId,
  'CreationDate', CreationDate
) FROM Votes;

ALTER TABLE VotesJson
  ADD COLUMN PostId INT UNSIGNED AS (Data->'$.PostId') STORED,
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id);

ALTER TABLE VotesJson
  ADD COLUMN UserId INT AS (NULLIF(Data->>'$.UserId', 'null')) STORED,
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ALTER TABLE VotesJson
  ADD COLUMN VoteTypeId TINYINT UNSIGNED AS (Data->'$.VoteTypeId') STORED,
  ADD FOREIGN KEY (VoteTypeId) REFERENCES VoteTypes(Id);

ANALYZE TABLE VotesJson;

/* ---------------------------------------------------------------------- */

SELECT COALESCE(TABLE_NAME, 'TOTAL') AS TABLE_NAME,
  ROUND(SUM(DATA_LENGTH+INDEX_LENGTH)/1024/1024, 2) AS MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='StackExchange'
GROUP BY TABLE_NAME WITH ROLLUP;

