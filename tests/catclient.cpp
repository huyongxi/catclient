#include <iostream>
#include <chrono>
#include <unistd.h>
#include <thread>
#include <random>
#include <vector>
#include <string>
#include <sstream>

#include "client.hpp"

using namespace std;

unsigned long long GetTime64() {
    return static_cast<unsigned long long int>(std::chrono::system_clock::now().time_since_epoch().count() / 1000);
}

void transaction() {
    cat::Transaction t("foo", "bar");
    t.AddData("foo", "1");
    t.AddData("bar", "2");
    t.AddData("foo is a bar");
    t.SetDurationStart(GetTime64() - 1000);
    t.SetTimestamp(GetTime64() - 1000);
    t.SetDurationInMillis(150);
    t.SetStatus(cat::FAIL);
    t.Complete();
}

void event() {
    cat::Event e("foo", "bar");
    e.AddData("foo", "1");
    e.AddData("bar", "2");
    e.AddData("foo is a bar");
    e.SetStatus(cat::SUCCESS);
    e.Complete();

    cat::logEvent("foo", "bar1");
    cat::logEvent("foo", "bar2", "failed");
    cat::logEvent("foo", "bar3", "failed", "k=v");
}

void metric() {
    cat::logMetricForCount("count");
    cat::logMetricForCount("count", 3);
    cat::logMetricForDuration("duration", 100);
}




int main() {
    cat::Config c = cat::Config();
    c.enableDebugLog = true;
    //c.encoderType = cat::ENCODER_TEXT;
    cat::init("huyongxi_test123", c);

    random_device rd;
    mt19937 gen(rd());

    uniform_int_distribution<> dis(1,10);
    normal_distribution<> nor(500,300);
    
    vector<thread> vec_thread;

    for(int i = 0; i < 1; ++i){
        vec_thread.emplace_back([&](){
            for(int i = 0; i < 1; ++i){
            stringstream ss;
            ss << this_thread::get_id();
            cat::Transaction t("call", ss.str().c_str());

            if(i%10000 == 0){
                cout << ss.str() << ":" << i << endl;
            }
            t.SetDurationInMillis(nor(gen));


            t.AddData("key"+ss.str(), ss.str().c_str());
            if(dis(gen) > 3){
                t.SetStatus(cat::SUCCESS);
            }else{
                t.SetStatus(cat::FAIL);
            }
            cat::logEvent("logevent 123456", ss.str().c_str());
            t.Complete();

            
        }
        });
    }
    

    usleep(1000000);

    for(auto& t : vec_thread){
        t.join();
    }
    cat::destroy();
}