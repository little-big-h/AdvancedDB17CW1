#include "Yelp-odb.hxx"
#include <algorithm>
#include <iostream>
#include <odb/database.hxx>
#include <odb/mssql/database.hxx>
#include <odb/transaction.hxx>

using std::cout;
using std::endl;
using odb::mssql::database;
using odb::transaction;
using odb::query;
using odb::result;
using std::to_string;

std::vector<std::string> findHours(odb::database& db, std::string username) {
	std::vector<std::string> result;
	transaction t(db.begin());
	for(auto& it : db.query<Review>(query<Review>::user_id->name == username)) {
		for(auto& h : it.business_id->hours) {
			result.push_back(h->hours);
		}
	}
	t.commit();
	return result;
}

std::vector<StarCount> countStars(odb::database& db, float latMin, float latMax, float longMin,
																	float longMax) {
	std::vector<StarCount> result;
	transaction t(db.begin());
	for(auto& it : db.query<StarCount>("select review.stars, count(*) from review, business where "
																		 "review.business_id = business.id and latitude between " +
																		 to_string(latMin) + " and " + to_string(latMax) +
																		 " and longitude between " + to_string(longMin) + " and " +
																		 to_string(longMax) + " group by review.stars")) {
		result.push_back(it);
	}
	t.commit();
	return result;
}

void createIndex(odb::database& db) {
	// Your implementation goes here:
	transaction t(db.begin());
	db.execute("create columnstore index csi on review (business_id, stars);");
	t.commit();

	// create a columnstore index to accelerate your query
}

void dropIndex(odb::database& db) {
	// Your implementation goes here:
	transaction t(db.begin());
	db.execute("drop index csi on review;");
	t.commit();
	// drop the columnstore index you've created
}

// ---------------------------------------------
// No need to change anything below this line
// ---------------------------------------------

LastQueryTime getLastQueryRuntime(odb::database& db) {
	transaction t(db.begin());
	auto time = db.query_one<LastQueryTime>();
	t.commit();
	return *time;
}

std::vector<std::string> hoursFixtureKn{
		"Friday|0:00-0:00",			 "Friday|0:00-0:00",			"Friday|11:00-1:00",
		"Friday|17:00-1:00",		 "Friday|17:00-1:00",			"Friday|17:30-23:00",
		"Friday|18:00-23:00",		 "Friday|8:00-2:00",			"Friday|9:00-2:00",
		"Monday|0:00-0:00",			 "Monday|0:00-0:00",			"Monday|11:00-23:00",
		"Monday|17:30-23:00",		 "Monday|18:00-23:00",		"Monday|8:00-2:00",
		"Monday|9:00-2:00",			 "Saturday|0:00-0:00",		"Saturday|0:00-0:00",
		"Saturday|11:00-1:00",	 "Saturday|11:30-23:00",	"Saturday|17:00-1:00",
		"Saturday|17:00-1:00",	 "Saturday|18:00-23:00",	"Saturday|8:00-4:00",
		"Saturday|9:00-2:00",		 "Sunday|0:00-0:00",			"Sunday|0:00-0:00",
		"Sunday|11:00-23:00",		 "Sunday|17:00-0:00",			"Sunday|17:00-0:00",
		"Sunday|17:30-23:00",		 "Sunday|18:00-23:00",		"Sunday|8:00-2:00",
		"Sunday|9:00-2:00",			 "Thursday|0:00-0:00",		"Thursday|0:00-0:00",
		"Thursday|11:00-23:00",	"Thursday|17:00-0:00",		"Thursday|17:00-0:00",
		"Thursday|17:30-23:00",	"Thursday|18:00-23:00",	"Thursday|8:00-2:00",
		"Thursday|9:00-2:00",		 "Tuesday|0:00-0:00",			"Tuesday|0:00-0:00",
		"Tuesday|11:00-23:00",	 "Tuesday|17:00-0:00",		"Tuesday|17:00-0:00",
		"Tuesday|17:30-23:00",	 "Tuesday|18:00-23:00",		"Tuesday|8:00-2:00",
		"Tuesday|9:00-2:00",		 "Wednesday|0:00-0:00",		"Wednesday|0:00-0:00",
		"Wednesday|11:00-23:00", "Wednesday|17:00-0:00",	"Wednesday|17:00-0:00",
		"Wednesday|17:30-23:00", "Wednesday|18:00-23:00", "Wednesday|8:00-2:00",
		"Wednesday|9:00-2:00"};

std::vector<std::string> hoursFixtureNeu{
		"Friday|11:30-2:00",		 "Friday|6:00-20:00",		 "Friday|9:00-19:00",		 "Monday|11:30-23:00",
		"Monday|6:00-20:00",		 "Monday|9:00-19:00",		 "Saturday|12:00-20:00", "Saturday|12:00-2:00",
		"Saturday|8:00-16:00",	 "Sunday|12:00-23:00",	 "Thursday|11:30-23:00", "Thursday|6:00-20:00",
		"Thursday|9:00-19:00",	 "Tuesday|11:30-23:00",	"Tuesday|6:00-20:00",	 "Tuesday|9:00-19:00",
		"Wednesday|11:30-23:00", "Wednesday|6:00-20:00", "Wednesday|9:00-19:00"};

std::vector<std::string> hoursFixtureEwr{"Friday|20:30-22:00", "Saturday|20:30-22:00",
																				 "Sunday|20:30-22:00", "Thursday|20:30-22:00"};

std::vector<StarCount> starFixture1{
		{1, 137039}, {2, 111817}, {3, 174317}, {4, 337639}, {5, 410518}};

std::vector<StarCount> starFixture2{{1, 28781}, {2, 19532}, {3, 27541}, {4, 56435}, {5, 83655}};

bool operator==(StarCount const& left, StarCount const& right) {
	return left.stars == right.stars && left.count == right.count;
};

int main(int argc, char** argv) {
	using namespace std;
	stringstream outstream;

	outstream << "{";
	if(argc > 1)
		(outstream << "\"student\": \"" << argv[1] << "\", ").flush();

	database db("SA", "AdvancedDB17", "yelp", "localhost");

	{ // testing find Hours
		auto hours = findHours(db, "kn");
		std::sort(hours.begin(), hours.end());
		outstream << "findHoursTest1: " << (hours == hoursFixtureKn ? "'passed', " : "'failed', ");
		hours = findHours(db, "neu");
		std::sort(hours.begin(), hours.end());
		outstream << "findHoursTest2: " << (hours == hoursFixtureNeu ? "'passed', " : "'failed', ");
		hours = findHours(db, "Ewr");
		std::sort(hours.begin(), hours.end());
		outstream << "findHoursTest3: " << (hours == hoursFixtureEwr ? "'passed', " : "'failed', ");
		hours = findHours(db, "Lucus");
		std::sort(hours.begin(), hours.end());
		outstream << "findHoursTest4: "
							<< (hours == vector<string>{"Friday|10:00-16:00",		"Friday|10:00-4:00",
																					"Friday|11:00-22:00",		"Monday|10:00-17:00",
																					"Monday|10:00-2:00",		"Monday|11:00-22:00",
																					"Saturday|10:00-17:00", "Saturday|10:00-4:00",
																					"Saturday|16:00-22:00", "Sunday|10:00-2:00",
																					"Sunday|16:00-22:00",		"Thursday|10:00-17:00",
																					"Thursday|10:00-2:00",	"Thursday|11:00-22:00",
																					"Tuesday|10:00-17:00",	"Tuesday|10:00-2:00",
																					"Tuesday|11:00-22:00",	"Wednesday|10:00-17:00",
																					"Wednesday|10:00-2:00", "Wednesday|11:00-22:00"}
											? "'passed', "
											: "'failed', ");
		hours = findHours(db, "Razoun");
		std::sort(hours.begin(), hours.end());
		outstream
				<< "findHoursTest5: "
				<< (hours ==
										vector<string>{
												"Friday|10:00-19:00",		 "Friday|11:00-2:00",			"Friday|11:30-23:00",
												"Friday|16:30-22:30",		 "Friday|17:30-22:30",		"Friday|5:30-23:00",
												"Friday|6:00-2:00",			 "Friday|9:00-18:00",			"Friday|9:00-19:00",
												"Monday|10:00-18:00",		 "Monday|11:00-2:00",			"Monday|11:30-22:00",
												"Monday|16:30-21:00",		 "Monday|17:30-22:30",		"Monday|5:30-23:00",
												"Monday|6:00-2:00",			 "Monday|9:00-19:00",			"Saturday|10:00-18:00",
												"Saturday|11:00-2:00",	 "Saturday|11:30-23:00",	"Saturday|16:30-22:30",
												"Saturday|17:30-22:30",	"Saturday|6:00-23:00",		"Saturday|6:00-2:00",
												"Saturday|9:00-18:00",	 "Saturday|9:00-19:00",		"Sunday|11:00-2:00",
												"Sunday|11:30-23:00",		 "Sunday|16:30-21:00",		"Sunday|6:00-2:00",
												"Sunday|6:30-22:00",		 "Sunday|9:00-19:00",			"Thursday|10:00-19:00",
												"Thursday|11:00-2:00",	 "Thursday|11:30-22:00",	"Thursday|16:30-21:00",
												"Thursday|17:30-22:30",	"Thursday|5:30-23:00",		"Thursday|6:00-2:00",
												"Thursday|9:00-18:00",	 "Thursday|9:00-19:00",		"Tuesday|10:00-18:00",
												"Tuesday|11:00-2:00",		 "Tuesday|11:30-22:00",		"Tuesday|16:30-21:00",
												"Tuesday|17:30-22:30",	 "Tuesday|5:30-23:00",		"Tuesday|6:00-2:00",
												"Tuesday|9:00-18:00",		 "Tuesday|9:00-19:00",		"Wednesday|10:00-18:00",
												"Wednesday|11:00-2:00",	"Wednesday|11:30-22:00", "Wednesday|16:30-21:00",
												"Wednesday|17:30-22:30", "Wednesday|5:30-23:00",	"Wednesday|6:00-2:00",
												"Wednesday|9:00-18:00",	"Wednesday|9:00-19:00"}
								? "'passed', "
								: "'failed', ");
		hours = findHours(db, "Vyjay");
		std::sort(hours.begin(), hours.end());
		outstream << "findHoursTest6: "
							<< (hours == vector<string>{"Friday|9:00-18:00", "Monday|9:00-18:00",
																					"Saturday|9:00-15:30", "Thursday|9:00-18:00",
																					"Tuesday|9:00-18:00", "Wednesday|9:00-18:00"}
											? "'passed', "
											: "'failed', ");
	}

	{ // testing countStars
		auto stars = countStars(db, 30.0, 45.7, -100.0, -1.0);
		std::sort(stars.begin(), stars.end(),
							[](auto left, auto right) { return left.stars < right.stars; });
		outstream << "countStarsTest1: " << (stars == starFixture1 ? "'passed', " : "'failed', ");
		stars = countStars(db, 4.0, 40., -90.0, -40.0);
		std::sort(stars.begin(), stars.end(),
							[](auto left, auto right) { return left.stars < right.stars; });
		outstream << "countStarsTest2: " << (stars == starFixture2 ? "'passed', " : "'failed', ");
		stars = countStars(db, 41.99999, 44.00001, -80.000001, -74.99999);
		std::sort(stars.begin(), stars.end(),
							[](auto left, auto right) { return left.stars < right.stars; });
		outstream << "countStarsTest3: "
							<< (stars ==
													std::vector<StarCount>{
															{1, 49472}, {2, 44610}, {3, 77029}, {4, 134906}, {5, 123642}}
											? "'passed', "
											: "'failed', ");
		stars = countStars(db, 32.99990, 34.00001, -112.49999, -111.50001);
		std::sort(stars.begin(), stars.end(),
							[](auto left, auto right) { return left.stars < right.stars; });
		outstream << "countStarsTest4: "
							<< (stars ==
													std::vector<StarCount>{
															{1, 167546}, {2, 86852}, {3, 104263}, {4, 224337}, {5, 526604}}
											? "'passed'"
											: "'failed'");
	}

	{ // performance runs

		try {
			// warmup run
			outstream << "\"TimesBeforeIndexing\": [";
			countStars(db, 30.0, 45.7, -100.0, -1.0);
			for(size_t i = 0; i < 5; i++) {
				countStars(db, 30.0, 45.7, -100.0, -1.0);
				outstream << getLastQueryRuntime(db).elapsed_time << (i < 4 ? ", " : "");
			}
			outstream << "], ";

			createIndex(db);

			outstream << "\"TimesAfterIndexing\": [";
			// warmup run
			countStars(db, 30.0, 45.7, -100.0, -1.0);
			for(size_t i = 0; i < 5; i++) {
				countStars(db, 30.0, 45.7, -100.0, -1.0);
				outstream << getLastQueryRuntime(db).elapsed_time << (i < 4 ? ", " : "");
			}
			outstream << "]";
		} catch(exception& e) {
			cerr << e.what() << endl;
		}

		dropIndex(db);
	}
	outstream << "}" << endl;
	cout << outstream.str();
	return 0;
}
