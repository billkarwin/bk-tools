/*
 * pt-full-ratio
 *
 * For all tables, report how "full" the primary key is.
 * I.e. how close the maximum value of an auto-increment primary key is
 * to overflowing the auto-increment data type.
 *
 * NOTE: the query below only reports against the 'test' schema.
 * You should use this query only as an example, and customize it.
 *
 * Copyright 2012 Bill Karwin
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
use test;
drop table if exists ti;
drop table if exists uti;
drop table if exists si;
drop table if exists usi;
drop table if exists mi;
drop table if exists umi;
drop table if exists i;
drop table if exists ui;
drop table if exists bi;
drop table if exists ubi;

create table ti (id tinyint auto_increment primary key) auto_increment=100;
insert into ti () values ();
create table uti (id tinyint unsigned auto_increment primary key) auto_increment=100;
insert into uti () values ();
create table si (id smallint auto_increment primary key) auto_increment=10000;
insert into si () values ();
create table usi (id smallint unsigned auto_increment primary key) auto_increment=10000;
insert into usi () values ();
create table mi (id mediumint auto_increment primary key) auto_increment=1000000;
insert into mi () values ();
create table umi (id mediumint unsigned auto_increment primary key) auto_increment=1000000;
insert into umi () values ();
create table i (id int auto_increment primary key) auto_increment=1000000000;
insert into i () values ();
create table ui (id int unsigned auto_increment primary key) auto_increment=1000000000;
insert into ui () values ();
create table bi (id bigint auto_increment primary key) auto_increment=10000000000000000;
insert into bi () values ();
create table ubi (id bigint unsigned auto_increment primary key) auto_increment=10000000000000000;
insert into ubi () values ();
*/

select concat('`', table_schema, '`.`', table_name, '`.`', column_name, '`') as `column`,
  auto_increment as `current_int`, max_int, round((auto_increment/max_int)*100, 2) as `pct_max`
from (select table_schema, table_name, column_name, auto_increment,
  pow(2, case data_type
    when 'tinyint'   then 7
    when 'smallint'  then 15
    when 'mediumint' then 23
    when 'int'       then 31
    when 'bigint'    then 63
    end+(column_type like '% unsigned'))-1 as max_int
  from information_schema.tables t
  join information_schema.columns c using (table_schema,table_name)
  join information_schema.key_column_usage k using (table_schema,table_name,column_name)
  where t.table_schema in ('test')
    and k.constraint_name = 'PRIMARY'
    and k.ordinal_position = 1
    and t.auto_increment is not null
) as dt;
