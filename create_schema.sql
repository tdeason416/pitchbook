use webcrawl;

CREATE TABLE http (
  id INT unsigned not null auto_increment,
  HTTP_response_code int not null unique, 
  primary key(id)
);

create table website (
  id int unsigned not null auto_increment,
  Website_URL varchar(100) not null unique, 
  primary key(id)
);

create table management (
  id int unsigned not null auto_increment,
  Management_link_URL varchar(100) not null unique, 
  Management_link_text varchar(100) not null, 
  primary key(id)
);

create table contact (
  id int unsigned not null auto_increment,
  Contact_link_URL varchar(100) not null unique, 
  Contact_link_text varchar(100), 
  primary key(id)
);

create table manager (
  id int unsigned not null auto_increment,
  Manager_name varchar(100) not null unique,
  Manager_title varchar(100) not null,
  primary key(id)
);

create table access (
  id int unsigned not null auto_increment,
  access_time datetime not null,
  http_id int unsigned not null,
  website_id int unsigned not null,
  management_id int unsigned not null,
  contact_id int unsigned not null,
  manager_id int unsigned not null,
  foreign key (http_id) references http(id) on delete cascade,
  foreign key (website_id) references website(id) on delete cascade,
  foreign key (management_id) references management(id) on delete cascade,
  foreign key (contact_id) references contact(id) on delete cascade,
  foreign key (manager_id) references manager(id) on delete cascade,
  primary key (id)
);

