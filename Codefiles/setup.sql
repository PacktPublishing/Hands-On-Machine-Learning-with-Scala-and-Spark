CREATE DATABASE forum;

DROP TABLE forum.b_messages;
CREATE TABLE IF NOT EXISTS forum.b_messages (
    message_id BIGINT NOT NULL,
    body VARCHAR(255) NOT NULL,
    author VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    datestamp BIGINT NOT NULL,
    PRIMARY KEY (message_id)
);

insert into forum.b_messages (message_id, body, author, subject, datestamp) values (1, "Hello, I am new here", "a", "Intro", 154495703);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (2, "Nice to meet you", "b", "Intro", 154495803);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (3, "What is a best weather to go to swimming?", "c", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (4, "Not a good man - what do you think", "d", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (5, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (6, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (7, "Hello, I am new here", "a", "Intro", 154495703);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (8, "Nice to meet you", "b", "Intro", 154495803);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (9, "What is a best weather to go to swimming?", "c", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (10, "Not a good man - what do you think", "d", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (11, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (12, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (13, "Hello, I am new here", "a", "Intro", 154495703);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (14, "Nice to meet you", "b", "Intro", 154495803);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (15, "What is a best weather to go to swimming?", "c", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (16, "Not a good man - what do you think", "d", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (17, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (18, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (19, "Hello, I am new here", "a", "Intro", 154495703);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (20, "Nice to meet you", "b", "Intro", 154495803);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (21, "What is a best weather to go to swimming?", "c", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (22, "Not a good man - what do you think", "d", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (23, "Not a good man - what do you think", "a", "Swimming", 154493303);
insert into forum.b_messages (message_id, body, author, subject, datestamp) values (24, "Not a good man - what do you think", "a", "Swimming", 154493303);