#include <memory>
#include <odb/core.hxx>
#include <odb/lazy-ptr.hxx>
#include <set>
#include <string>

#pragma db view
class StarCount {
public:
	int stars;
	int count;
};

#pragma db view query(                                                                             \
		"select top 1 text, last_elapsed_time from sys.dm_exec_query_stats cross apply sys.dm_exec_sql_text(sql_handle) order by last_execution_time desc")
class LastQueryTime {
public:
	std::string text;
	long elapsed_time;
};

// ---------------------------------------------
// No need to change anything above this line
// ---------------------------------------------

class Review;
class Business;

#pragma db object table("User")
class Duh {
public:
#pragma db id
	std::string id;
	std::string name;
	int review_count;
#pragma db inverse(user_id)
	std::set<std::unique_ptr<Review>> reviewsV;
};

#pragma db object readonly
class Hours {
public:
#pragma db id
	int id;
	odb::lazy_shared_ptr<Business> business_id;
	std::string hours;
};

#pragma db object
class Review {
public:
#pragma db id
	std::string id;
	odb::lazy_shared_ptr<Duh> user_id;
	std::shared_ptr<Business> business_id;
	std::string text;
};

#pragma db object
class Business {
public:
#pragma db id
	std::string id;
#pragma db inverse(business_id)
	std::set<std::unique_ptr<Hours>> hours;
	std::string name;
};
