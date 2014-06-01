#include <kpp_protocol.hpp>
#include <iostream>
#include <iomanip>

struct test{
	int * holder;
	test() {
		holder = new int();
		std::cout << "test() : " << std::hex << holder << std::endl;
	}
 
	test(test&& old) : holder(nullptr) {
		std::cout << "test(test&&) old: " << std::hex << old.holder << " new: " << holder << std::endl;
		std::swap(holder,old.holder);
	}
	test(const test& old) {
		holder = new int(*old.holder);
		std::cout << "test(const test&) : " << std::hex << holder << std::endl;
	}
	~test()
	{
		std::cout << "~test() : " << std::hex << holder << std::endl;
		delete holder;
	}
};


int main(){

  using namespace std;
  std::cout << "4" << std::endl;

  uint32_t x = 0x0ABCDEF0;
  uint32_t y = endian::hton(x);
  uint32_t z = endian::ntoh(y);

  cout << hex << setw(8) << setfill('0') << x << " " << y << " " << setw(8) << setfill('0') << z << endl;

	using my_var = variant::variant<std::string, test>;
	
	my_var d;
	
	d.set<std::string>("First string");
	std::cout << d.get<std::string>() << std::endl;
  std::cout << "1" << std::endl;
	d.set<test>();
	*d.get<test>().holder = 42;
  std::cout << "2" << std::endl;
	my_var e(std::move(d));
	std::cout << dec << *e.get<test>().holder << std::endl;    
  std::cout << "3" << std::endl;
 
	*e.get<test>().holder = 43;
  
  std::cout << "4" << std::endl;
 
	d = e;
  
  std::cout << "5" << std::endl;
	
	std::cout << *d.get<test>().holder << std::endl;    

}

