#include <iostream>
#include <string>
#include <sstream>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <thread>
#include <chrono>
#include "json.hpp"
#include "mqtt/async_client.h"

#include <pqxx/pqxx>


using namespace pqxx;
using namespace std;

const string POST_TOPIC { "POST" };

const string GET_TOPIC_1 { "GET_1" };
const string GET_TOPIC_2 { "GET_2" };

const string DELETE_TOPIC_1 { "DELETE_1" };
const string DELETE_TOPIC_2 { "DELETE_2" };

const string SERVER_ADDRESS	{ "tcp://localhost:1883" };
const string CLIENT_ID		{ "paho_cpp_async_consume" };


const int  QOS = 1;


const auto TIMEOUT = std::chrono::seconds(10);

/////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    connection C("dbname = postgres user = admin password = 12345pass hostaddr = 127.0.0.1 port = 5432");
    if (C.is_open())
    {
        cout << "Opened database successfully: " << C.dbname() << endl;
        try
        {
            work W(C);
            W.exec( "CREATE TABLE IF NOT EXISTS test1 (event_name VARCHAR(64), event_desciption VARCHAR(64));");
            W.commit();
        }
        catch(...)
        {
            cout << "Something went wrong with test1 table creation..." << endl;
        }
    } else {
       cout << "Can't open database" << endl;
       return 1;
    }



    mqtt::async_client cli(SERVER_ADDRESS, CLIENT_ID);

    auto connOpts = mqtt::connect_options_builder()
        .clean_session(false)
        .finalize();

    try
    {
        cout << "Connecting to the MQTT server..." << flush;
        auto tok = cli.connect(connOpts);

        auto rsp = tok->get_connect_response();

        cli.subscribe(POST_TOPIC, QOS)->wait();
        cli.subscribe(GET_TOPIC_1, QOS)->wait();
        cli.subscribe(DELETE_TOPIC_1, QOS)->wait();
        cli.start_consuming();
        cout << "OK" << endl;
    }
    catch (const mqtt::exception& exc)
    {
        cerr << "\n  " << exc << endl;
        return 1;
    }

    try
    {
        cout << "Waiting for messages ...'"<< endl;

        while (true)
        {
            auto msg = cli.consume_message();
            if (!msg) break;
//          nlohmann::json j = nlohmann::json::parse(msg->to_string().c_str());

            if(msg->get_topic() == GET_TOPIC_1)
            {
                cout << "Message from " << GET_TOPIC_1 << " received ..."<< endl;
                cout << msg->get_topic() <<  msg->to_string() << endl;
                stringstream query;
                query << "SELECT event_desciption FROM test1 ";
                query << "WHERE event_name = '" << msg->to_string() << "';";
                cout << query.str().c_str() << endl;
                nontransaction N(C);
                result R( N.exec( query ));
                string reply;
                for (result::const_iterator c = R.begin(); c != R.end(); ++c)
                {
                    cout << "ID = " << c[0].as<string>() << endl;
                    if(!reply.empty()) reply.append(";");
                    reply.append(c[0].as<string>());
                }
                mqtt::message_ptr pubmsg = mqtt::make_message(GET_TOPIC_2, reply);
                pubmsg->set_qos(QOS);
                cli.publish(pubmsg)->wait_for(TIMEOUT);
                cout << "...Ok" << endl;
            }

            if(msg->get_topic() == POST_TOPIC)
            {
                cout << "Message from " << POST_TOPIC << " received ..."<< endl;
                nlohmann::json j = nlohmann::json::parse(msg->to_string().c_str());
                cout << msg->get_topic() << ": " << j["key"] <<": " << j["value"] << endl;
                stringstream query;
                query << "INSERT INTO test1 (event_name, event_desciption) ";
                query << "VALUES ('"<< j["key"].get<std::string>() <<"','"<< j["value"].get<std::string>() <<"');";
                cout << query.str().c_str() << endl;
                work W(C);
                W.exec( query.str().c_str() );
                W.commit();
                cout << "...Ok" << endl;
            }

            if(msg->get_topic() == DELETE_TOPIC_1)
            {
                cout << "Message from " << DELETE_TOPIC_1 << " received ..."<< endl;
                nlohmann::json j = nlohmann::json::parse(msg->to_string().c_str());
                cout << msg->get_topic() << ": " << j["key"] << endl;
                stringstream query;
                query << "DELETE FROM test1 ";
                query << "WHERE event_name = '" <<  j["key"].get<std::string>() << "';";
                cout << query.str().c_str() << endl;
                work W(C);
                W.exec( query.str().c_str() );
                W.commit();
//                mqtt::message_ptr pubmsg = mqtt::make_message(DELETE_TOPIC_2, reply);
//                pubmsg->set_qos(QOS);
//                cli.publish(pubmsg)->wait_for(TIMEOUT);

                cout << "...Ok" << endl;
            }
        }

        if (cli.is_connected())
        {
            cout << "\nShutting down and disconnecting from the MQTT server..." << flush;
            cli.unsubscribe(POST_TOPIC)->wait();
            cli.unsubscribe(GET_TOPIC_1)->wait();
            cli.unsubscribe(DELETE_TOPIC_1)->wait();
            cli.stop_consuming();
            cli.disconnect()->wait();
            cout << "...OK" << endl;
        }
        else
        {
            cout << "\nClient was disconnected" << endl;
        }
    }
    catch (const mqtt::exception& exc)
    {
        cerr << "\n  " << exc << endl;
        return 1;
    }

    return 0;
}

