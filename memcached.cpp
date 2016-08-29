//
// memcached.cpp
// ~~~~~~~~~~~~~~~~~~~~~~~~~
// memcached programming exercise from Steve Sanders, April 27, 2015
// steve@zanderz.com
// Partially inspired by a boost.asio example
// Compiled on windows with boost 1.58, written to be cross platform.
//
// Usage: memcached [port]
// 
// This implementation makes use of boost asio for object oriented
// asynchronous cross-platform networking.
// An acceptor registers a handler for incoming connections, which 
// creates a new session object for each.
// Each session starts by reading a memcache header, then reading
// a body of the specified size, and writing a response.
// If no errors occur, a new header read is begun.
//
// Key-value pairs are stored in a std::map, protected by a mutex.
// This mutex is only held during reading and writing of the map,
// not any network operations, for good throughput.
//
// Performance and limitations:
// Minimal error checking has been implemented.  No size bounds are enforced.
// Scale should be bound by the maximum number of socket connections and
// memory of the system.
// Network buffers are copied to temporary values before map reading/writing,
// which could be improved with other techniques such as refcounted buffers
//
// Python bmemcached testing
// Note that authentication is not supported.
//
//	  import bmemcached
//	  client = bmemcached.Client(('127.0.0.1:11211', ))
//	  client.set('key', 'value')
//	  print client.get('key')
//

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <boost/asio.hpp>
#include <boost/asio/impl/src.hpp>
#include <vector>
#include <map>
#include <mutex>

using boost::asio::ip::tcp;

using itemType = std::vector<unsigned char>;
// Note that the 4 bytes of flags are prepended to each value
std::map<itemType, itemType> g_itemCache;
// Mutex is only held during map reading and writing
std::mutex g_cacheMutex;


class session
	: public std::enable_shared_from_this < session >
{
public:
	session(tcp::socket socket)
		: socket_(std::move(socket))
	{
	}

	void start()
	{
		do_read_header();
	}

private:

	// Read an incoming command once the socket is open and the session object has been created
	void do_read_header()
	{
		// These calls establish a reference to keep the session object alive
		// until the handlers go out of scope
		auto self(shared_from_this());
		async_read(socket_, boost::asio::buffer(header_, header_length),
			[this, self](boost::system::error_code ec, std::size_t /*length*/)
		{
			// Test for magic byte, plus get or set
			if (!ec && header_[0] == 0x80 && (header_[1] == 0x0 || header_[1] == 0x1))
			{
				do_read_body();
			}
		});
	}

	void do_read_body()
	{
		auto self(shared_from_this());
		size_t bodySize = (header_[8] << 24) + (header_[9] << 16) + (header_[10] << 8) + header_[11];
		buffer_.resize(bodySize);
		async_read(socket_, boost::asio::buffer(&(buffer_[0]), bodySize),
			[this, self](boost::system::error_code ec, std::size_t length)
		{
			if (!ec)
			{
				if (header_[1] == 0x0)	// get
					do_get();
				else if(header_[1] == 0x1)	// set
					do_set();
			}
		});
	}

	void do_get()
	{
		{
			// Notice we only lock this thread after the socket read has completed, and before
			// a write is attempted.
			std::lock_guard<std::mutex> lock(g_cacheMutex);
			// header_[4] == extra length
			itemType::const_iterator keyStart = buffer_.begin() + header_[4];
			// header_[2], header_[3] == key size
			size_t keySize = (header_[2] << 8) + header_[3];
			auto result = g_itemCache.find(itemType(keyStart, keyStart + keySize));
			if (result != g_itemCache.end())
				buffer_ = result->second;		// yes, make a copy
			else
				buffer_.clear();
			// unlock
		}
		send_response(buffer_.size()?0:1);
	}

	void do_set()
	{
		{
			std::lock_guard<std::mutex> lock(g_cacheMutex);
			// header_[4] == extra length
			itemType::const_iterator keyStart = buffer_.begin() + header_[4];
			// header_[2], header_[3] == key size
			size_t keySize = (header_[2] << 8) + header_[3];
			itemType::const_iterator valStart = keyStart + keySize;
			// Prepend the flags
			itemType tempValue(buffer_.begin(), buffer_.begin() + 4);
			tempValue.insert(tempValue.end(), valStart, buffer_.cend());
			g_itemCache[itemType(keyStart, keyStart + keySize)] = std::move(tempValue);
			// unlock
		}
		buffer_.clear();
		send_response(0);
	}

	//Response Status
	//	Possible values 
	//	0x0000  No error
	//	0x0001  Key not found
	//	0x0002  Key exists
	void send_response(unsigned char status)
	{
		size_t bodyLength = buffer_.size();
		unsigned char extraLength = 0;
		if (status == 0 && buffer_.size())
			extraLength = 4;
		if (status == 1)
			buffer_ = { 'N', 'o', 't', ' ', 'f', 'o', 'u', 'n', 'd' };
		// Put the header in the start of the buffer, so we can reply with
		// one async write
		buffer_.insert(buffer_.begin(), header_length, 0);
		buffer_[0] = 0x81;	// magic
		buffer_[4] = extraLength;
		buffer_[6] = 0;
		buffer_[7] = status;	// 6,7 are status
		// 8-11 total body length
		buffer_[8] = (bodyLength) >> 24;
		buffer_[9] = (bodyLength) >> 16;
		buffer_[10] = (bodyLength) >> 8;
		buffer_[11] = (bodyLength) & 0xff;

		auto self(shared_from_this());
		boost::asio::async_write(socket_, boost::asio::buffer(&(buffer_[0]), buffer_.size()),
			[this, self](boost::system::error_code ec, std::size_t /*length*/)
		{
			if (!ec)
			{
				// If everything is OK, keep the conversation going
				do_read_header();
			}
		});
	}

	// Access to these member variables is serialized by the order 
	// of the reads and writes.
	tcp::socket socket_;
	enum { header_length = 24 };
	unsigned char header_[header_length];
	unsigned char flags_[4];
	itemType buffer_;
};

// For each incoming connection, this server creates a new session
// object and starts it
class server
{
public:
	server(boost::asio::io_service& io_service, short port)
		: acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
		socket_(io_service)
	{
		do_accept();
	}

private:
	void do_accept()
	{
		acceptor_.async_accept(socket_,
			[this](boost::system::error_code ec)
		{
			if (!ec)
			{
				std::make_shared<session>(std::move(socket_))->start();
			}

			do_accept();
		});
	}

	tcp::acceptor acceptor_;
	tcp::socket socket_;
};

int main(int argc, char* argv[])
{
	try
	{
		if (argc != 2)
		{
			std::cerr << "Usage: memcached <port>\n";
			return 1;
		}

		boost::asio::io_service io_service;

		server s(io_service, std::atoi(argv[1]));

		io_service.run();
	}
	catch (std::exception& e)
	{
		std::cerr << "Exception: " << e.what() << "\n";
	}

	return 0;
}