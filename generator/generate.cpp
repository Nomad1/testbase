#include <time.h>
#include <iostream>
#include <string>

using namespace std;

int main(int argc, const char * argv[])
{
	string path;	
	if(argc >= 2)
        	path = argv[1];
	else
	{
		cout << "PATH is empty\n";
		return -1;
	}

	string from = argc >= 5 ? argv[4] : "2014-10-31 00:00";
	string to = argc >= 6 ? argv[5] : "2014-10-31 23:59";
	int offset = 60;
	int ips = argc >= 3 ? atoi(argv[2]) : 1000;
	int cpus = argc >= 4 ? atoi(argv[3]) : 2;

	struct tm timeDate;
	memset(&timeDate, 0, sizeof(struct tm));
	strptime(from.c_str(), "%Y-%m-%d %H:%M", &timeDate);
	int startTs = mktime(&timeDate);

	memset(&timeDate, 0, sizeof(struct tm));
	strptime(to.c_str(), "%Y-%m-%d %H:%M", &timeDate);

	int endTs = mktime(&timeDate);

	int count = ((endTs - startTs) / offset + 1) * ips * cpus;
	cout << "Generating " << count << " records and " << ips << " files\n";

	//unsigned char rnd[count];
	//FILE * fr = fopen("/dev/urandom", "r");
	//fread(&rnd, 1, count, fr);
	//fclose(fr);

	int i = 0;
	int ip = 0;

	while (ip < ips)
	{
		char ipstr[100];
		sprintf(ipstr, "%d.%d.%d.%d", (ip/16516350) + 10, (ip/64770) % 255, (ip/254) % 255, (ip % 254) + 1);
		cout << "Working with " << ipstr << "\n";
		string ipfile;
		ipfile += path; 
		ipfile += "/";
		ipfile += ipstr;
		ipfile += ".log";
 
		remove(ipfile.c_str());

		FILE * f = fopen(ipfile.c_str(), "w");

		int ts = startTs;
		while (ts < endTs)
		{
			int cpu = 0;
			while (cpu < cpus)
			{
				int load = rand() % 101; //rnd[i] % 101;
				fprintf(f, "%d %s %d %d\n", ts, ipstr, cpu, load);
				cpu++;
				i++;
			}
			ts += offset;
		}
		fclose(f);
		ip++;
	}
}