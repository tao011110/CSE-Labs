#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"
#include "marshall.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		int taskType;
		int index;
		vector<string> files;
	};

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
		mr_tasktype taskType;
		int index;
	};

};

marshall &
operator<<(marshall &m, mr_protocol::AskTaskResponse reply){
	m << reply.taskType;
	m << reply.index;
	m << reply.files;

	return m;
}

unmarshall &
operator>>(unmarshall &u, mr_protocol::AskTaskResponse &reply){
	u >> reply.taskType;
	u >> reply.index;
	u >> reply.files;

	return u;
}

#endif

