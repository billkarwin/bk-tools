/* ----------------------------------------------------------------------
 *
 * Load XML data from the Stack Exchange data dump, which is Creative Commons
 * https://archive.org/details/stackexchange
 *
 * Remember you need to enable local LOAD INFILE commands with the `local_infile=ON` option in /etc/my.cnf
 * and also use the same option with the client:
 *
 * mysql --local-infile < Base.sql
 */

CREATE DATABASE IF NOT EXISTS StackExchange;
USE StackExchange;

DROP TABLE IF EXISTS StackExchange.PostTags;
DROP TABLE IF EXISTS StackExchange.Tags;
DROP TABLE IF EXISTS StackExchange.VotesXml;
DROP TABLE IF EXISTS StackExchange.Votes;
DROP TABLE IF EXISTS StackExchange.VoteTypes;
DROP TABLE IF EXISTS StackExchange.PostHistory;
DROP TABLE IF EXISTS StackExchange.PostHistoryTypes;
DROP TABLE IF EXISTS StackExchange.CloseReasonTypes;
DROP TABLE IF EXISTS StackExchange.Comments;
DROP TABLE IF EXISTS StackExchange.Posts;
DROP TABLE IF EXISTS StackExchange.PostTypes;
DROP TABLE IF EXISTS StackExchange.BadgesXml;
DROP TABLE IF EXISTS StackExchange.Badges;
DROP TABLE IF EXISTS StackExchange.BadgeTypes;
DROP TABLE IF EXISTS StackExchange.Users;

SET @DATETIME_ISO8601 = '%Y-%m-%dT%H:%i:%s.%f';

/* ---------------------------------------------------------------------- */

CREATE TABLE Users (
  Id               INT AUTO_INCREMENT PRIMARY KEY,
  Reputation       INT UNSIGNED NOT NULL DEFAULT 1,
  CreationDate     DATETIME NOT NULL,
  DisplayName      TINYTEXT NOT NULL,
  LastAccessDate   DATETIME NOT NULL,
  WebsiteUrl       VARCHAR(200) NULL,
  Location         TINYTEXT,
  Age              TINYINT UNSIGNED NULL,
  AboutMe          TEXT NULL,
  Views            INT UNSIGNED NOT NULL DEFAULT 0,
  UpVotes          INT UNSIGNED NOT NULL DEFAULT 0,
  DownVotes        INT UNSIGNED NOT NULL DEFAULT 0
);

LOAD XML LOCAL INFILE 'Users.xml' INTO TABLE Users
(Id, Reputation, @CreationDate, DisplayName, @LastAccessDate, WebsiteUrl, Location, Age, AboutMe, Views, UpVotes, DownVotes)
SET CreationDate = STR_TO_DATE(@CreationDate, @DATETIME_ISO8601),
    LastAccessDate = STR_TO_DATE(@LastAccessDate, @DATETIME_ISO8601);

ANALYZE TABLE Users;

/* ---------------------------------------------------------------------- */

CREATE TABLE BadgesXml (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  UserId           INT NOT NULL,
  Name             TINYTEXT NOT NULL,
  Date             TINYTEXT NULL,
  Class            SMALLINT UNSIGNED NOT NULL,
  TagBased         TINYTEXT NOT NULL
);

LOAD XML LOCAL INFILE 'Badges.xml' INTO TABLE BadgesXml
(Id, UserId, Name, Date, Class, TagBased);

CREATE TABLE BadgeTypes (
  Id               SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Description      TINYTEXT NOT NULL,
  TagBased         TINYINT(1) NOT NULL DEFAULT FALSE
);

INSERT INTO BadgeTypes (Description, TagBased) SELECT DISTINCT Name, TagBased='True' FROM BadgesXml;

ANALYZE TABLE BadgeTypes;

CREATE TABLE Badges (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  BadgeTypeId      SMALLINT UNSIGNED NOT NULL,
  UserId           INT NOT NULL,
  CreationDate     DATETIME NULL
);

INSERT INTO Badges (Id, BadgeTypeId, UserId, CreationDate)
SELECT b.Id, t.Id, b.UserId, STR_TO_DATE(b.Date, @DATETIME_ISO8601)
FROM BadgesXml AS b JOIN BadgeTypes AS t ON b.Name=t.Description;

DROP TABLE BadgesXml;

ALTER TABLE Badges
  ADD FOREIGN KEY (BadgeTypeId) REFERENCES BadgeTypes(Id),
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ANALYZE TABLE Badges;

/* ---------------------------------------------------------------------- */

CREATE TABLE PostTypes (
  Id               TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Description      VARCHAR(32) NOT NULL
);

INSERT INTO PostTypes (Id, Description) VALUES
  (1, 'Question'),
  (2, 'Answer'),
  (3, 'Wiki'),
  (4, 'TagWikiExcerpt'),
  (5, 'TagWiki'),
  (6, 'ModeratorNomination'),
  (7, 'WikiPlaceholder'),
  (8, 'PrivilegeWiki');

ANALYZE TABLE PostTypes;

CREATE TABLE Posts (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  PostTypeId       TINYINT UNSIGNED NOT NULL,
  AcceptedAnswerId INT UNSIGNED NULL, -- only if PostTypeId = 1
  ParentId         INT UNSIGNED NULL, -- only if PostTypeId = 2
  CreationDate     DATETIME NOT NULL,
  Score            SMALLINT NOT NULL DEFAULT 0,
  ViewCount        INT UNSIGNED NOT NULL DEFAULT 0,
  Body             TEXT NOT NULL,
  OwnerUserId      INT NULL,
  LastEditorUserId INT NULL,
  LastEditDate     DATETIME NULL,
  LastActivityDate DATETIME NULL,
  Title            TINYTEXT NOT NULL,
  Tags             TINYTEXT NOT NULL,
  AnswerCount      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  CommentCount     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  FavoriteCount    SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  ClosedDate       DATETIME NULL
);

LOAD XML LOCAL INFILE 'Posts.xml' INTO TABLE Posts
(Id, PostTypeId, AcceptedAnswerId, ParentId, @CreationDate, Score, ViewCount, Body, OwnerUserId, LastEditorUserId, @LastEditDate, @LastActivityDate, Title, Tags, AnswerCount, CommentCount, FavoriteCount, @ClosedDate)
SET CreationDate = STR_TO_DATE(@CreationDate, @DATETIME_ISO8601),
    LastEditDate = STR_TO_DATE(@LastEditDate, @DATETIME_ISO8601),
    LastActivityDate = STR_TO_DATE(@LastActivityDate, @DATETIME_ISO8601),
    ClosedDate = STR_TO_DATE(@ClosedDate, @DATETIME_ISO8601);

ALTER TABLE Posts
  ADD FOREIGN KEY (PostTypeId) REFERENCES PostTypes(Id),
  ADD FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (ParentId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (OwnerUserId) REFERENCES Users(Id),
  ADD FOREIGN KEY (LastEditorUserId) REFERENCES Users(Id);

ANALYZE TABLE Posts;

CREATE OR REPLACE VIEW Questions AS SELECT * FROM Posts WHERE PostTypeId = 1;
CREATE OR REPLACE VIEW Answers   AS SELECT * FROM Posts WHERE PostTypeId = 2;

/* ---------------------------------------------------------------------- */

CREATE TABLE Comments (
  Id               INT UNSIGNED PRIMARY KEY,
  PostId           INT UNSIGNED NOT NULL,
  Score            INT NOT NULL,
  Text             TEXT NOT NULL,
  CreationDate     DATETIME NOT NULL,
  UserId           INT NOT NULL
);

LOAD XML LOCAL INFILE 'Comments.xml' INTO TABLE Comments
(Id, PostId, Score, Text, @CreationDate, UserId)
SET CreationDate = STR_TO_DATE(@CreationDate, @DATETIME_ISO8601);

DELETE c FROM Comments c LEFT JOIN Users u ON c.UserId=u.Id WHERE u.Id IS NULL;

ALTER TABLE Comments
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ANALYZE TABLE Comments;

/* ---------------------------------------------------------------------- */

CREATE TABLE PostHistoryTypes (
  Id               TINYINT UNSIGNED PRIMARY KEY,
  Description      TINYTEXT NOT NULL
);

INSERT INTO PostHistoryTypes VALUES
  (1, 'Initial Title - The first title a question is asked with.'),
  (2, 'Initial Body - The first raw body text a post is submitted with.'),
  (3, 'Initial Tags - The first tags a question is asked with.'),
  (4, 'Edit Title - A question''s title has been changed.'),
  (5, 'Edit Body - A post''s body has been changed, the raw text is stored here as markdown.'),
  (6, 'Edit Tags - A question''s tags have been changed.'),
  (7, 'Rollback Title - A question''s title has reverted to a previous version.'),
  (8, 'Rollback Body - A post''s body has reverted to a previous version - the raw text is stored here.'),
  (9, 'Rollback Tags - A question''s tags have reverted to a previous version.'),
  (10, 'Post Closed - A post was voted to be closed.'),
  (11, 'Post Reopened - A post was voted to be reopened.'),
  (12, 'Post Deleted - A post was voted to be removed.'),
  (13, 'Post Undeleted - A post was voted to be restored.'),
  (14, 'Post Locked - A post was locked by a moderator.'),
  (15, 'Post Unlocked - A post was unlocked by a moderator.'),
  (16, 'Community Owned - A post has become community owned.'),
  (17, 'Post Migrated - A post was migrated.'),
  (18, 'Question Merged - A question has had another, deleted question merged into itself.'),
  (19, 'Question Protected - A question was protected by a moderator'),
  (20, 'Question Unprotected - A question was unprotected by a moderator'),
  (21, 'Post Disassociated - An admin removes the OwnerUserId from a post.'),
  (22, 'Question Unmerged - A previously merged question has had its answers and votes restored.'),
  (24, 'Suggested Edit Applied'),
  (25, 'Post Tweeted'),
  (31, 'Discussion moved to c'),
  (33, 'Post Notice Added'),
  (34, 'Post Notice Removed'),
  (35, 'Post Migrated Away'),
  (36, 'Post Migrated Here'),
  (37, 'Post Merge Source'),
  (38, 'Post Merge Destination'),
  (50, 'Community Bump');

ANALYZE TABLE PostHistoryTypes;

CREATE TABLE CloseReasonTypes (
  Id               TINYINT UNSIGNED PRIMARY KEY,
  Name             TINYTEXT NOT NULL
);

INSERT INTO CloseReasonTypes VALUES
  (1, 'exact duplicate'),
  (2, 'off topic'),
  (3, 'not constructive'),
  (4, 'not a real question'),
  (7, 'too localized'),
  (10, 'general reference'),
  (20, 'noise of pointless'),
  (101, 'duplicate'),
  (102, 'off-topic'),
  (103, 'unclear what you''re asking'),
  (104, 'too broad'),
  (105, 'primary opinion-based');

ANALYZE TABLE CloseReasonTypes;

CREATE TABLE PostHistory (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  PostHistoryTypeId TINYINT UNSIGNED NOT NULL,
  PostId           INT UNSIGNED NOT NULL,
  RevisionGUID     CHAR(36) CHARACTER SET ascii NOT NULL,
  CreationDate     DATETIME NOT NULL,
  UserId           INT NULL,
  Text             TEXT NULL
);

LOAD XML LOCAL INFILE 'PostHistory.xml' INTO TABLE PostHistory
(Id, PostHistoryTypeId, PostId, RevisionGUID, @CreationDate, UserId, Text)
SET CreationDate = STR_TO_DATE(@CreationDate, @DATETIME_ISO8601);

DELETE h FROM PostHistory h LEFT JOIN Users u ON h.PostHistoryTypeId=u.Id WHERE u.Id IS NULL;

ALTER TABLE PostHistory
  ADD FOREIGN KEY (PostHistoryTypeId) REFERENCES PostHistoryTypes(Id),
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id);

ANALYZE TABLE PostHistory;

/* ---------------------------------------------------------------------- */

CREATE TABLE VoteTypes (
  Id               TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  Description      TINYTEXT NOT NULL
);

INSERT INTO VoteTypes (Id, Description) VALUES
  (1, 'AcceptedByOriginator'),
  (2, 'UpMod'),
  (3, 'DownMod'),
  (4, 'Offensive'),
  (5, 'Favorite'),
  (6, 'Close'),
  (7, 'Reopen'),
  (8, 'BountyStart'),
  (9, 'BountyClose'),
  (10, 'Deletion'),
  (11, 'Undeletion'),
  (12, 'Spam'),
  (15, 'ModeratorReview'),
  (16, 'ApproveEditSuggestion');

ANALYZE TABLE VoteTypes;

CREATE TABLE VotesXml (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  PostId           INT UNSIGNED NOT NULL,
  VoteTypeId       TINYINT UNSIGNED NOT NULL,
  UserId           TINYTEXT,
  CreationDate     TINYTEXT
);

LOAD XML LOCAL INFILE 'Votes.xml' INTO TABLE VotesXml
(Id, PostId, VoteTypeId, UserId, CreationDate);

CREATE TABLE Votes (
  Id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  PostId           INT UNSIGNED NOT NULL,
  VoteTypeId       TINYINT UNSIGNED NOT NULL,
  UserId           INT NULL, -- only if VoteTypeId = 5
  CreationDate     DATETIME NOT NULL
);

INSERT INTO Votes (Id, PostId, VoteTypeId, UserId, CreationDate)
SELECT v.Id, v.PostId, v.VoteTypeId, v.UserId, STR_TO_DATE(v.CreationDate, @DATETIME_ISO8601)
FROM VotesXml AS v;

DROP TABLE VotesXml;

DELETE v FROM Votes AS v LEFT JOIN Posts AS p ON v.PostId=p.Id WHERE p.Id IS NULL;

ALTER TABLE Votes
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (UserId) REFERENCES Users(Id),
  ADD FOREIGN KEY (VoteTypeId) REFERENCES VoteTypes(Id);

ANALYZE TABLE Votes;

/* ---------------------------------------------------------------------- */

/*

CREATE TABLE Tags (
  Id               SMALLINT UNSIGNED PRIMARY KEY,
  Tag              VARCHAR(32) NOT NULL
);

ANALYZE TABLE Tags;

CREATE TABLE PostTags (
  PostId           INT UNSIGNED NOT NULL,
  TagId            SMALLINT UNSIGNED NOT NULL,
  PRIMARY KEY (PostId, TagId)
);

ALTER TABLE PostTags
  ADD FOREIGN KEY (PostId) REFERENCES Posts(Id),
  ADD FOREIGN KEY (TagId) REFERENCES Tags(Id);

ANALYZE TABLE PostTags;

*/

/* ---------------------------------------------------------------------- */

SELECT COALESCE(TABLE_NAME, 'TOTAL') AS TABLE_NAME,
  ROUND(SUM(DATA_LENGTH+INDEX_LENGTH)/1024/1024, 2) AS MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='StackExchange'
GROUP BY TABLE_NAME WITH ROLLUP;


