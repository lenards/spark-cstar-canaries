USE canary_ks;

INSERT INTO kv (key, value) VALUES (1, 'One');
INSERT INTO kv (key, value) VALUES (5, 'Five');
INSERT INTO kv (key, value) VALUES (6, 'Six');
INSERT INTO kv (key, value) VALUES (7, 'Seven');
INSERT INTO kv (key, value) VALUES (9, 'Nine');
INSERT INTO kv (key, value) VALUES (10, 'Ten');
INSERT INTO kv (key, value) VALUES (12, 'Twelve');
INSERT INTO kv (key, value) VALUES (16, 'Sixteen');
INSERT INTO kv (key, value) VALUES (19, 'Nineteen');



INSERT INTO  kv2 (key, ts, value)
VALUES (500, '2015-03-30 08:50:11Z', '89.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (501, '2015-03-30 08:50:12Z', '89.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (502, '2015-03-30 08:50:13Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (503, '2015-03-30 08:50:14Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (504, '2015-03-30 08:50:15Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (505, '2015-03-30 08:50:16Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (506, '2015-03-30 08:50:17Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (507, '2015-03-30 08:50:18Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (508, '2015-03-30 08:50:19Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (509, '2015-03-30 08:50:20Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (510, '2015-03-30 08:50:21Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (511, '2015-03-30 08:50:22Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (512, '2015-03-30 08:50:23Z', '89.5 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (513, '2015-03-30 08:50:24Z', '89.5 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (514, '2015-03-30 08:50:25Z', '89.8 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (515, '2015-03-30 08:50:26Z', '89.6 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (516, '2015-03-30 08:50:27Z', '89.9 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (517, '2015-03-30 08:50:28Z', '99.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (518, '2015-03-30 08:50:29Z', '89.6 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (519, '2015-03-30 08:50:30Z', '89.7 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (520, '2015-03-30 08:50:31Z', '89.7 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:11Z', '89.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:12Z', '89.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:13Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:14Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:15Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:16Z', '89.1 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:17Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:18Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:19Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:20Z', '89.2 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:21Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:22Z', '89.3 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:23Z', '89.5 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:24Z', '89.5 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:25Z', '89.8 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:26Z', '89.6 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:27Z', '89.9 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:28Z', '99.0 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:29Z', '89.6 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:30Z', '89.7 F');
INSERT INTO  kv2 (key, ts, value)
VALUES (650, '2015-03-30 08:50:31Z', '89.7 F');

USE canary;

INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 1);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 4);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 2);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 6);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 1);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 7);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 8);
INSERT INTO eventrecord (key, cmpkey, ts, event, num) VALUES ('test', '651', dateOf(now()), 'read', 15);
