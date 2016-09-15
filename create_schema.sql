USE webcrawl;

create table http (
      id int unsigned not null auto_increment,
      response int not null,
      primary key(id)
);

create table website (
      id int unsigned not null auto_increment,
      Website_URL varchar(30) not null,
      primary key(id)
);

create table management (
      id int unsigned not null auto_increment,
      Management_link_URL varchar(30) not null,
      Management_link_text varchar(30) not null,
      primary key(id)
);

create table contact (
      id int unsigned not null auto_increment,
      Management_link_URL varchar(30) not null,
      Management_link_text varchar(30) not null,
      primary key(id)
);

create table manager (
      id int unsigned not null auto_increment,
      Management_link_URL varchar(30) not null,
      Management_link_text varchar(30) not null,
      primary key(id)
);

create table manager_title (
      id int unsigned not null auto_increment,
      Manager_title varchar(30) not null,
      primary key(id)
);

create table web_status (
      id int unsigned not null auto_increment,
      access_time datetime not null,
      http_id int unsigned not null,
      web_id int unsigned not null,
      mgmt_link_id int unsigned not null,
      contact_link_id int unsigned not null,
      manager_id int unsigned not null,
      manager_title_id int unsigned not null,
      foreign key (http_id) references HTTP_code(id) on delete cascade,
      foreign key (web_id) references website(id) on delete cascade,
      foreign key (mgmt_link_id) references management(id) on delete cascade,
      foreign key (contact_link_id) references contact(id) on delete cascade,
      foreign key (manager_id) references manager(id) on delete cascade,
      foreign key (manager_title_id) references manager_title(id) on delete cascade,
      primary key(id)
);


