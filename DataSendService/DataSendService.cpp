#include <windows.h>
#include <winsock.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#include <stdlib.h>
#include <TlHelp32.h>
#include <sal.h>
#include <thread>
#include <time.h>

#define DEBUG 0
#define DEFAULT_REMOTE_IP "10.125.193.65"
#define DEFAULT_REMOTE_PORT 7200
#define REG_KEY_DIR TEXT("SOFTWARE\\VehicleLocation")

/*******************************************/
/* Macro to call ODBC functions and        */
/* report an error on failure.             */
/* Takes handle, handle type, and stmt     */
/*******************************************/

#define TRYODBC(h, ht, x)   {   RETCODE rc = x;\
                                if (rc != SQL_SUCCESS) \
                                { \
                                    HandleDiagnosticRecord (h, ht, rc); \
                                } \
                                if (rc == SQL_ERROR) \
                                { \
                                    fwprintf(stderr, L"Error in " L#x L"\n"); \
                                    goto Exit;  \
                                }  \
                            }
/******************************************/
/* Structure to store information about   */
/* a column.
/******************************************/

typedef struct STR_BINDING {
    SQLSMALLINT         cDisplaySize;           /* size to display  */
    WCHAR               *wszBuffer;             /* display buffer   */
    SQLLEN              indPtr;                 /* size or null     */
    BOOL                fChar;                  /* character col?   */
    struct STR_BINDING  *sNext;                 /* linked list      */
} BINDING;



/******************************************/
/* Forward references                     */
/******************************************/

void HandleDiagnosticRecord (SQLHANDLE      hHandle,    
                 SQLSMALLINT    hType,  
                 RETCODE        RetCode);

void DisplayResults(HSTMT       hStmt,
                    SQLSMALLINT cCols);

void AllocateBindings(HSTMT         hStmt,
                      SQLSMALLINT   cCols,
                      BINDING**     ppBinding,
                      SQLSMALLINT*  pDisplay);


void DisplayTitles(HSTMT    hStmt,
                   DWORD    cDisplaySize,
                   BINDING* pBinding);

void SetConsole(DWORD   cDisplaySize,
                BOOL    fInvert);

#define TAIL_BYTES 2
typedef struct {
	char serial[10 + TAIL_BYTES];
	char time[6 + TAIL_BYTES];
	char date[6 + TAIL_BYTES];
	double latitude;
	double longitude;
	USHORT speed;
	USHORT direction;
	USHORT height;
	USHORT accuracy;
	char vehicle_status[8 + TAIL_BYTES];
	char usr_alarm_flag[2 + TAIL_BYTES];
	long id;
} ParsedRecord;

static DWORD QueryMaxID(void)
{
	HKEY hKey;
	int iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, REG_KEY_DIR, 0, NULL, 0, KEY_QUERY_VALUE, NULL, &hKey, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key for read [errno %d]\n", iResult);
		return NULL; // assume max ID is 0 on query failure
	}

	DWORD max_id;
	DWORD regKeyType;
	DWORD length = sizeof(long);
	iResult = RegQueryValueEx(hKey, L"MaxSentID", 0, &regKeyType, (BYTE *)&max_id, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD) {
		fprintf(stderr, "Registry key MaxSentID does not exist [errno %d]\n", iResult);
		return NULL;
	}
	return max_id;
}

static bool SaveMaxID(DWORD max_id)
{
	HKEY hKey;
	int iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, REG_KEY_DIR, 0, NULL, 0, KEY_SET_VALUE, NULL, &hKey, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key for write [errno %d]\n", iResult);
		return false;
	}
	iResult = RegSetValueEx(hKey, L"MaxSentID", 0, REG_DWORD, (BYTE *)&max_id, sizeof(max_id));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Set registry key failed [errno %d]\n", iResult);
		return false;
	}
	return true;
}

#pragma pack(1)
typedef struct {
	USHORT magic;
	USHORT command;
	USHORT version;
	int    body_length;
} recordHeader;

typedef struct {
	recordHeader header;
	char   serial[20];
	double longitude;
	double latitude;
	USHORT speed;
	USHORT direction;
	USHORT height;
	USHORT accuracy;
	USHORT year;
	BYTE   month;
	BYTE   day;
	BYTE   hour;
	BYTE   minute;
	BYTE   second;
} sendRecord;

typedef struct {
	recordHeader header;
	char   token[50];
} authRequest;

typedef struct {
	recordHeader header;
	USHORT authResult;
	char   message[50];
} authResponse;

typedef struct {
	recordHeader header;
} heartbeatPacket;

static SOCKET global_ClientSocket = INVALID_SOCKET;
static long global_max_id = NULL;
static sockaddr_in global_UDPDest;
static bool global_UseUDP = false;

static long String2Int(char *str, int str_len)
{
	long result = 0;
	for (int i = 0; i < str_len; i++) {
		result *= 10;
		result += str[i] - '0';
	}
	return result;
}

static int sendton(
  _In_  SOCKET s,
  _In_  const char *buf,
  _In_  int len,
  _In_  int flags,
  _In_  const struct sockaddr *to,
  _In_  int tolen
) {
	int left_len = len;
	while (left_len > 0) {
		int recvBytes = sendto(s, buf, left_len, flags, to, tolen);
		if (recvBytes <= 0)
			return recvBytes;
		buf += recvBytes;
		left_len -= recvBytes;
	}
	return len;
}

static int sendn(
  _In_  SOCKET s,
  _In_  const char *buf,
  _In_  int len,
  _In_  int flags
) {
	int left_len = len;
	while (left_len > 0) {
		int recvBytes = send(s, buf, left_len, flags);
		if (recvBytes <= 0)
			return recvBytes;
		buf += recvBytes;
		left_len -= recvBytes;
	}
	return len;
}

static bool send_parsed_record(ParsedRecord *pr)
{
	sendRecord r;

	r.header.magic = 0xAAAA;
	r.header.command = 0xCCCC;
	r.header.version = htons(0x0002); // different from the document
	r.header.body_length = htonl(sizeof(r) - sizeof(r.header));

	memset(r.serial, 0, sizeof(r.serial));
	memcpy(r.serial, pr->serial, sizeof(pr->serial));
	r.longitude = pr->longitude;
	r.latitude = pr->latitude;
	r.speed = htons(pr->speed);
	r.direction = htons(pr->direction);
	r.height = htons(pr->height);
	r.accuracy = htons(pr->accuracy);

	struct tm timeinfo;
	timeinfo.tm_sec = String2Int(pr->time + 4, 2);
	timeinfo.tm_min = String2Int(pr->time + 2, 2);
	timeinfo.tm_hour = String2Int(pr->time, 2);
	timeinfo.tm_isdst = 0;
	timeinfo.tm_mday = String2Int(pr->date, 2);
	timeinfo.tm_mon = String2Int(pr->date + 2, 2) - 1;
	timeinfo.tm_year = 2000 + String2Int(pr->date + 4, 2) - 1900;
	time_t local_timestamp = mktime(&timeinfo) + 8 * 3600; // UTC +8
	struct tm local_timeinfo;
	localtime_s(&local_timeinfo, &local_timestamp);

	r.year = (USHORT)(local_timeinfo.tm_year + 1900); // this field is little endian instead of network order
	r.month = (BYTE)(local_timeinfo.tm_mon + 1);
	r.day = (BYTE)local_timeinfo.tm_mday;
	r.hour = (BYTE)local_timeinfo.tm_hour;
	r.minute = (BYTE)local_timeinfo.tm_min;
	r.second = (BYTE)local_timeinfo.tm_sec;

	fprintf(stderr, "New record: id=%d serial=%.20s longitude=%lf latitude=%lf speed=%d direction=%d "
		"timestamp=%d year=%d month=%d day=%d hour=%d minute=%d second=%d\n",
		pr->id, r.serial, r.longitude, r.latitude, ntohs(r.speed), ntohs(r.direction),
		(int)local_timestamp, r.year, r.month, r.day, r.hour, r.minute, r.second);

	if (global_UseUDP) {
		if (sizeof(sendRecord) != sendton(global_ClientSocket, (char*)&r, sizeof(sendRecord), 0, (sockaddr *)&global_UDPDest, sizeof(global_UDPDest))) {
			fprintf(stderr, "UDP Socket send failure\n");
			return false;
		}
	} else {
		if (sizeof(sendRecord) != sendn(global_ClientSocket, (char *)&r, sizeof(sendRecord), 0)) {
			fprintf(stderr, "TCP Socket send failure\n");
			return false;
		}
	}
	return true;
}

static RETCODE handle_sql_data(SQLHSTMT hStmt)
{
	while (SQLFetch(hStmt) == SQL_SUCCESS) {
		ParsedRecord pr;
		memset(&pr, 0, sizeof(pr));
		SQLGetData(hStmt, 1, SQL_C_CHAR, pr.serial, sizeof(pr.serial), NULL);
		SQLGetData(hStmt, 2, SQL_C_CHAR, pr.time, sizeof(pr.time), NULL);
		SQLGetData(hStmt, 3, SQL_C_CHAR, pr.date, sizeof(pr.date), NULL);
		SQLGetData(hStmt, 4, SQL_C_DOUBLE, &pr.latitude, sizeof(pr.latitude), NULL);
		SQLGetData(hStmt, 5, SQL_C_DOUBLE, &pr.longitude, sizeof(pr.longitude), NULL);
		SQLGetData(hStmt, 6, SQL_C_USHORT, &pr.speed, sizeof(pr.speed), NULL);
		SQLGetData(hStmt, 7, SQL_C_USHORT, &pr.direction, sizeof(pr.direction), NULL);
		SQLGetData(hStmt, 8, SQL_C_USHORT, &pr.height, sizeof(pr.height), NULL);
		SQLGetData(hStmt, 9, SQL_C_USHORT, &pr.accuracy, sizeof(pr.accuracy), NULL);
		SQLGetData(hStmt, 10, SQL_C_CHAR, pr.vehicle_status, sizeof(pr.vehicle_status), NULL);
		SQLGetData(hStmt, 11, SQL_C_CHAR, pr.usr_alarm_flag, sizeof(pr.usr_alarm_flag), NULL);
		SQLGetData(hStmt, 12, SQL_C_LONG, &pr.id, sizeof(pr.id), NULL);

#if DEBUG
		fprintf(stderr, "New row in database: serial=%s time=%s date=%s latitude=%lf longitude=%lf speed=%d direction=%d "
			"height=%d accuracy=%d vehicle_status=%s usr_alarm_flag=%s id=%ld\n",
			pr.serial, pr.time, pr.date, pr.latitude, pr.longitude, pr.speed, pr.direction, pr.height, pr.accuracy, pr.vehicle_status, pr.usr_alarm_flag, pr.id);
#endif

		if (!send_parsed_record(&pr)) {
			SaveMaxID((DWORD)global_max_id);
			return SQL_ERROR;
		}
		
		if (pr.id > global_max_id)
			global_max_id = pr.id;
	}
	SaveMaxID((DWORD)global_max_id);
	return SQL_SUCCESS;
}

static RETCODE sql_query_wrapper(SQLHSTMT hStmt, WCHAR *wszInput)
{
	RETCODE RetCode = SQL_ERROR;

	RetCode = SQLExecDirect(hStmt, wszInput, SQL_NTS);

    switch(RetCode)
    {
    case SQL_SUCCESS_WITH_INFO:
        {
            HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, RetCode);
            // fall through
        }
    case SQL_SUCCESS:
        {
			SQLSMALLINT sNumResults;
            TRYODBC(hStmt,
                    SQL_HANDLE_STMT,
                    SQLNumResultCols(hStmt,&sNumResults));
			// If this is a row-returning query, handle the data
            if (sNumResults > 0)
            {
				if (SQL_ERROR == handle_sql_data(hStmt)) {
					RetCode = SQL_ERROR;
					goto Exit;
				}
			}
            break;
        }

    case SQL_ERROR:
        {
            HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, RetCode);
            break;
        }

    default:
        fwprintf(stderr, L"Unexpected return code %hd!\n", RetCode);

    }

Exit:
    TRYODBC(hStmt,
            SQL_HANDLE_STMT,
            SQLFreeStmt(hStmt, SQL_CLOSE));
	return RetCode;
}

static SOCKET socket_connect(const char* IPAddress, const u_short PortNo)
{
    //Start up Winsock?
    WSADATA wsadata;
	
    //Fill out the information needed to initialize a socket?
    SOCKADDR_IN target; //Socket address information

    int error = WSAStartup(0x0202, &wsadata);

    //Did something happen?
    if (error)
		return INVALID_SOCKET;

    //Did we get the right Winsock version?
    if (wsadata.wVersion != 0x0202)
    {
		return INVALID_SOCKET;
    }

    SOCKET ClientSocket = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP); //Create socket
    if (ClientSocket == INVALID_SOCKET)
    {
		return INVALID_SOCKET; //Couldn't create the socket
    }

	target.sin_family = AF_INET; // address family Internet
    target.sin_port = htons (PortNo); //Port to connect on
    target.sin_addr.s_addr = inet_addr (IPAddress); //Target IP

    //Try connecting...
    if (connect(ClientSocket, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
    {
		closesocket(ClientSocket);
		return INVALID_SOCKET; //Couldn't connect
    }
	return ClientSocket; //Success
}


/*****************************************/
/* Some constants                        */
/*****************************************/


#define DISPLAY_MAX 50          // Arbitrary limit on column width to display
#define DISPLAY_FORMAT_EXTRA 3  // Per column extra display bytes (| <data> )
#define DISPLAY_FORMAT      L"%c %*.*s "
#define DISPLAY_FORMAT_C    L"%c %-*.*s "
#define NULL_SIZE           6   // <NULL>
#define SQL_QUERY_SIZE      1000 // Max. Num characters for SQL Query passed in.

#define PIPE                L'|'

SHORT   gHeight = 80;       // Users screen height

#define SQL_MAXLEN 1024
static RETCODE poll_database(SQLHSTMT hStmt, SOCKET ClientSocket)
{
	global_ClientSocket = ClientSocket;
	global_max_id = QueryMaxID();
	while (1) {
		WCHAR sql[SQL_MAXLEN] = {0};
		wsprintf(sql, L"SELECT serial,time,date,latitude,longitude,speed,direction,height,accuracy,vehicle_status,usr_alarm_flag,id FROM location WHERE id > %ld ORDER BY id", global_max_id);
		if (SQL_ERROR == sql_query_wrapper(hStmt, sql))
			return SQL_ERROR;
		Sleep(1000);
	}
}

// return received size on success, return error code of recv on error
static int recvn(SOCKET ClientSocket, char *buf, size_t size)
{
	int remaining_size = size;
	while (remaining_size > 0) {
		int iResult = recv(ClientSocket, buf, remaining_size, 0);
		if (iResult <= 0)
			return iResult;
		remaining_size -= iResult;
		buf += iResult;
	}
	return size;
}

static bool do_client_authentication(SOCKET ClientSocket)
{
	authRequest req;
	req.header.magic = 0xAAAA;
	req.header.command = 0xBBBB;
	req.header.version = htons(0x2200);
	req.header.body_length = sizeof(req) - sizeof(req.header);

	memset(req.token, 0, sizeof(req.token));
	HKEY hKey;
	int iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, REG_KEY_DIR, 0, NULL, 0, KEY_QUERY_VALUE, NULL, &hKey, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key for read [errno %d]\n", iResult);
		return false;
	}
	DWORD regKeyType;
	DWORD length = sizeof(req.token);
	iResult = RegQueryValueExA(hKey, "RemoteAuthToken", 0, &regKeyType, (LPBYTE)req.token, &length);
	if (length > sizeof(req.token)) {
		fprintf(stderr, "RemoteAuthToken too long (%d bytes)", length);
		return false;
	}
	if (iResult == ERROR_MORE_DATA) {
		iResult = RegQueryValueExA(hKey, "RemoteAuthToken", 0, &regKeyType, (LPBYTE)req.token, &length);
	}
	if (iResult != ERROR_SUCCESS || regKeyType != REG_SZ) {
		fprintf(stderr, "Registry key RemoteAuthToken does not exist [errno %d]\n", iResult);
		return false;
	}
	fprintf(stderr, "Token: %s\n", req.token);

	if (sizeof(req) != send(ClientSocket, (char *)&req, sizeof(req), 0)) {
		fprintf(stderr, "authentication packet send failed\n");
		return false;
	}

	authResponse res;
	if (sizeof(res) != recvn(ClientSocket, (char *)&res, sizeof(res))) {
		fprintf(stderr, "authentication packet recv failed\n");
		return false;
	}
	if (res.header.magic != 0xAAAA || res.header.command != htons(0xBBCC)) {
		fprintf(stderr, "invalid auth response\n");
		return false;
	}
	if (res.authResult == 0) {
		fprintf(stderr, "authentication failed, remote msg: %.50s\n", res.message);
		return false;
	}
	printf("Authentication succeeded\n");

	return true;
}

static void thread_reply_heartbeat(SOCKET ClientSocket)
{
	while (true) {
		heartbeatPacket h;
		if (sizeof(h) != recvn(ClientSocket, (char *)&h, sizeof(h))) {
			return;
		}
		if (sizeof(h) != send(ClientSocket, (char *)&h, sizeof(h), 0)) {
			return;
		}
	}
}

static bool isMyselfRunning(void)
{
	TCHAR szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, MAX_PATH );
	TCHAR *szBaseName = wcsrchr(szFileName, L'\\');
	if (szBaseName == NULL)
		szBaseName = szFileName;
	else
		szBaseName++; // skip \\

	// Take a snapshot of all processes in the system.
	HANDLE hProcessSnap = CreateToolhelp32Snapshot( TH32CS_SNAPPROCESS, 0 );
	if (hProcessSnap == INVALID_HANDLE_VALUE) {
		fprintf(stderr, "Error CreateToolhelp32Snapshot (of processes)\n");
		return false;
	}

	PROCESSENTRY32 pe32;
	pe32.dwSize = sizeof( PROCESSENTRY32 );
	if (!Process32First(hProcessSnap, &pe32)) {
		fprintf(stderr, "Error Process32First\n");
		CloseHandle( hProcessSnap );
		return false;
	}

	int procNum = 0;
	do {
		if (lstrcmpW(pe32.szExeFile, szBaseName) == 0) {
			procNum++;
		}
	} while (Process32Next(hProcessSnap, &pe32));
	CloseHandle(hProcessSnap);

	return (procNum > 1); // procNum counts myself
}

void error(WCHAR *msg)
{
	MessageBoxW(GetConsoleWindow(), msg, L"Error", 0);
}

int __cdecl wmain(void)
{
    SQLHENV     hEnv = NULL;
    SQLHDBC     hDbc = NULL;
    SQLHSTMT    hStmt = NULL;
    WCHAR      *pwszConnStr = new WCHAR[SQL_MAXLEN];
    WCHAR       wszInput[SQL_QUERY_SIZE];

	if (isMyselfRunning()) {
		error(L"Another instance of this service is already running!");
		goto Exit;
	}

    // Allocate an environment

    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv) == SQL_ERROR)
    {
        error(L"Unable to allocate an environment handle\n");
        exit(-1);
    }

    // Register this as an application that expects 3.x behavior,
    // you must register something if you use AllocHandle

    TRYODBC(hEnv,
            SQL_HANDLE_ENV,
            SQLSetEnvAttr(hEnv,
                SQL_ATTR_ODBC_VERSION,
                (SQLPOINTER)SQL_OV_ODBC3,
                0));

    // Allocate a connection
    TRYODBC(hEnv,
            SQL_HANDLE_ENV,
            SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc));

	HKEY hKey;
	int iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, REG_KEY_DIR, 0, NULL, 0, KEY_QUERY_VALUE, NULL, &hKey, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key for read [errno %d]\n", iResult);
		goto Exit;
	}
	DWORD regKeyType;
	DWORD length = SQL_MAX_LENGTH;
	iResult = RegQueryValueEx(hKey, L"DBConnString", 0, &regKeyType, (LPBYTE)pwszConnStr, &length);
	if (iResult == ERROR_MORE_DATA) {
		pwszConnStr = new WCHAR[length];
		iResult = RegQueryValueEx(hKey, L"DBConnString", 0, &regKeyType, (LPBYTE)pwszConnStr, &length);
	}
	if (iResult != ERROR_SUCCESS || regKeyType != REG_SZ) {
		fprintf(stderr, "Registry key DBConnString does not exist [errno %d]\n", iResult);
		goto Exit;
	}

    // Connect to the driver.  Use the connection string if supplied
    // on the input, otherwise let the driver manager prompt for input.

    TRYODBC(hDbc,
        SQL_HANDLE_DBC,
        SQLDriverConnect(hDbc,
                         NULL,
                         pwszConnStr,
                         SQL_NTS,
                         NULL,
                         0,
                         NULL,
                         SQL_DRIVER_COMPLETE));

    fwprintf(stderr, L"Connected!\n");

    TRYODBC(hDbc,
            SQL_HANDLE_DBC,
            SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));

	if (SQL_ERROR == sql_query_wrapper(hStmt, L"USE VehicleLocation")) {
		fprintf(stderr, "Database does not exist, exiting\n");
		goto Exit;
	}

	WCHAR *w_remote_ip = new WCHAR[20];
	char remote_ip[20] = DEFAULT_REMOTE_IP;
	length = 20;
	iResult = RegQueryValueEx(hKey, L"RemoteIP", 0, &regKeyType, (LPBYTE)w_remote_ip, &length);
	if (iResult == ERROR_MORE_DATA) {
		w_remote_ip = new WCHAR[length];
		iResult = RegQueryValueEx(hKey, L"RemoteIP", 0, &regKeyType, (LPBYTE)w_remote_ip, &length);
	}
	if (iResult != ERROR_SUCCESS || regKeyType != REG_SZ) {
		fprintf(stderr, "Registry key RemoteIP does not exist [errno %d], using default: %s\n", iResult, remote_ip);
	} else {
		WideCharToMultiByte(CP_ACP, 0, w_remote_ip, 20, remote_ip, 20, 0, 0);
	}

	u_short portno = DEFAULT_REMOTE_PORT;
	DWORD w_portno;
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"RemotePort", 0, &regKeyType, (LPBYTE)&w_portno, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD) {
		fprintf(stderr, "Registry key RemotePort does not exist [errno %d], using default: %d\n", iResult, portno);
	} else {
		portno = (USHORT)w_portno;
	}

	DWORD SkipAuthentication;
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"SkipAuthentication", 0, &regKeyType, (LPBYTE)&SkipAuthentication, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD)
		SkipAuthentication = 0;

	DWORD NeverHideWindow;
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"NeverHideWindow", 0, &regKeyType, (LPBYTE)&NeverHideWindow, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD)
		NeverHideWindow = 0;

	DWORD UseUDP;
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"UseUDP", 0, &regKeyType, (LPBYTE)&UseUDP, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD)
		UseUDP = 0;
	global_UseUDP = (UseUDP != 0);
	if (global_UseUDP) {
		while (1) {
			fprintf(stderr, "Using UDP connection to %s:%d\n", remote_ip, portno);
			global_UDPDest.sin_family = AF_INET;
			global_UDPDest.sin_addr.s_addr = inet_addr(remote_ip);
			global_UDPDest.sin_port = htons(portno);

			SOCKET ClientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			if (!NeverHideWindow)
				ShowWindow(GetConsoleWindow(), SW_HIDE);

			poll_database(hStmt, ClientSocket);
			if (!NeverHideWindow)
				ShowWindow(GetConsoleWindow(), SW_SHOW);
		}
	}

	while (1) {
		SOCKET ClientSocket = socket_connect(remote_ip, portno);
		if (ClientSocket != INVALID_SOCKET) {
			fprintf(stderr, "Connected to remote socket %s:%d\n", remote_ip, portno);
			// if SkipAuthentication registry is on, enter DB main loop directly
			if (SkipAuthentication || do_client_authentication(ClientSocket)) {
				std::thread *thread = new std::thread(thread_reply_heartbeat, ClientSocket);
				thread->detach();
				if (!NeverHideWindow)
					ShowWindow(GetConsoleWindow(), SW_HIDE);
				poll_database(hStmt, ClientSocket); // loop until socket or database connection fails
				if (!NeverHideWindow)
					ShowWindow(GetConsoleWindow(), SW_SHOW);
			}
			closesocket(ClientSocket);
		}
		fprintf(stderr, "Remote socket connection failed, try to reconnect in 1 second\n");
		Sleep(1000); // wait 1000ms and reconnect
	}

    wprintf(L"Enter SQL commands, type (control)Z to exit\nSQL COMMAND>");

    // Loop to get input and execute queries

    while(_fgetts(wszInput, SQL_QUERY_SIZE-1, stdin))
    {
        RETCODE     RetCode;
        SQLSMALLINT sNumResults;

        // Execute the query

        if (!(*wszInput))
        {
            wprintf(L"SQL COMMAND>");
            continue;
        }
        RetCode = SQLExecDirect(hStmt,wszInput, SQL_NTS);

        switch(RetCode)
        {
        case SQL_SUCCESS_WITH_INFO:
            {
                HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, RetCode);
                // fall through
            }
        case SQL_SUCCESS:
            {
                // If this is a row-returning query, display
                // results
                TRYODBC(hStmt,
                        SQL_HANDLE_STMT,
                        SQLNumResultCols(hStmt,&sNumResults));

                if (sNumResults > 0)
                {
                    DisplayResults(hStmt,sNumResults);
                } 
                else
                {
                    SQLLEN cRowCount;

                    TRYODBC(hStmt,
                        SQL_HANDLE_STMT,
                        SQLRowCount(hStmt,&cRowCount));

                    if (cRowCount >= 0)
                    {
                        wprintf(L"%Id %s affected\n",
                                 cRowCount,
                                 cRowCount == 1 ? L"row" : L"rows");
                    }
                }
                break;
            }

        case SQL_ERROR:
            {
                HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, RetCode);
                break;
            }

        default:
            fwprintf(stderr, L"Unexpected return code %hd!\n", RetCode);

        }
        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLFreeStmt(hStmt, SQL_CLOSE));

        wprintf(L"SQL COMMAND>");
    }

Exit:

    // Free ODBC handles and exit

    if (hStmt)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    }

    if (hDbc)
    {
        SQLDisconnect(hDbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    }

    if (hEnv)
    {
        SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
    }

    wprintf(L"\nDisconnected.");

    return 0;

}

/************************************************************************
/* DisplayResults: display results of a select query
/*
/* Parameters:
/*      hStmt      ODBC statement handle
/*      cCols      Count of columns
/************************************************************************/

void DisplayResults(HSTMT       hStmt,
                    SQLSMALLINT cCols)
{
    BINDING         *pFirstBinding, *pThisBinding;          
    SQLSMALLINT     cDisplaySize;
    RETCODE         RetCode = SQL_SUCCESS;
    int             iCount = 0;

    // Allocate memory for each column 

    AllocateBindings(hStmt, cCols, &pFirstBinding, &cDisplaySize);

    // Set the display mode and write the titles

    DisplayTitles(hStmt, cDisplaySize+1, pFirstBinding);


    // Fetch and display the data

    bool fNoData = false;

    do {
        // Fetch a row

        if (iCount++ >= gHeight - 2)
        {
            int     nInputChar;
            bool    fEnterReceived = false;

            while(!fEnterReceived)
            {   
                wprintf(L"              ");
                SetConsole(cDisplaySize+2, TRUE);
                wprintf(L"   Press ENTER to continue, Q to quit (height:%hd)", gHeight);
                SetConsole(cDisplaySize+2, FALSE);

                nInputChar = _getch();
                wprintf(L"\n");
                if ((nInputChar == 'Q') || (nInputChar == 'q'))
                {
                    goto Exit;
                }
                else if ('\r' == nInputChar)
                {
                    fEnterReceived = true;
                }
                // else loop back to display prompt again
            }

            iCount = 1;
            DisplayTitles(hStmt, cDisplaySize+1, pFirstBinding);
        }

        TRYODBC(hStmt, SQL_HANDLE_STMT, RetCode = SQLFetch(hStmt));

        if (RetCode == SQL_NO_DATA_FOUND)
        {
            fNoData = true;
        }
        else
        {

            // Display the data.   Ignore truncations

            for (pThisBinding = pFirstBinding;
                pThisBinding;
                pThisBinding = pThisBinding->sNext)
            {
                if (pThisBinding->indPtr != SQL_NULL_DATA)
                {
                    wprintf(pThisBinding->fChar ? DISPLAY_FORMAT_C:DISPLAY_FORMAT,
                        PIPE,
                        pThisBinding->cDisplaySize,
                        pThisBinding->cDisplaySize,
                        pThisBinding->wszBuffer);
                } 
                else
                {
                    wprintf(DISPLAY_FORMAT_C,
                        PIPE,
                        pThisBinding->cDisplaySize,
                        pThisBinding->cDisplaySize,
                        L"<NULL>");
                }
            }
            wprintf(L" %c\n",PIPE);
        }
    } while (!fNoData);

    SetConsole(cDisplaySize+2, TRUE);
    wprintf(L"%*.*s", cDisplaySize+2, cDisplaySize+2, L" ");
    SetConsole(cDisplaySize+2, FALSE);
    wprintf(L"\n");

Exit:
    // Clean up the allocated buffers

    while (pFirstBinding)
    {
        pThisBinding = pFirstBinding->sNext;
        free(pFirstBinding->wszBuffer);
        free(pFirstBinding);
        pFirstBinding = pThisBinding;
    }
}

/************************************************************************
/* AllocateBindings:  Get column information and allocate bindings
/* for each column.  
/*
/* Parameters:
/*      hStmt      Statement handle
/*      cCols       Number of columns in the result set
/*      *lppBinding Binding pointer (returned)
/*      lpDisplay   Display size of one line
/************************************************************************/

void AllocateBindings(HSTMT         hStmt,
                      SQLSMALLINT   cCols,
                      BINDING       **ppBinding,
                      SQLSMALLINT   *pDisplay)
{
    SQLSMALLINT     iCol;
    BINDING         *pThisBinding, *pLastBinding = NULL;
    SQLLEN          cchDisplay, ssType;
    SQLSMALLINT     cchColumnNameLength;

    *pDisplay = 0;

    for (iCol = 1; iCol <= cCols; iCol++)
    {
        pThisBinding = (BINDING *)(malloc(sizeof(BINDING)));
        if (!(pThisBinding))
        {
            fwprintf(stderr, L"Out of memory!\n");
            exit(-100);
        }

        if (iCol == 1)
        {
            *ppBinding = pThisBinding;
        }
        else
        {
            pLastBinding->sNext = pThisBinding;
        }
        pLastBinding = pThisBinding;


        // Figure out the display length of the column (we will
        // bind to char since we are only displaying data, in general
        // you should bind to the appropriate C type if you are going
        // to manipulate data since it is much faster...)

        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLColAttribute(hStmt,
                    iCol,
                    SQL_DESC_DISPLAY_SIZE,
                    NULL,
                    0,
                    NULL,
                    &cchDisplay));


        // Figure out if this is a character or numeric column; this is
        // used to determine if we want to display the data left- or right-
        // aligned.

        // SQL_DESC_CONCISE_TYPE maps to the 1.x SQL_COLUMN_TYPE. 
        // This is what you must use if you want to work
        // against a 2.x driver.

        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLColAttribute(hStmt,
                    iCol,
                    SQL_DESC_CONCISE_TYPE,
                    NULL,
                    0,
                    NULL,
                    &ssType));


        pThisBinding->fChar = (ssType == SQL_CHAR ||
                                ssType == SQL_VARCHAR ||
                                ssType == SQL_LONGVARCHAR);

        pThisBinding->sNext = NULL;

        // Arbitrary limit on display size
        if (cchDisplay > DISPLAY_MAX)
            cchDisplay = DISPLAY_MAX;

        // Allocate a buffer big enough to hold the text representation
        // of the data.  Add one character for the null terminator

        pThisBinding->wszBuffer = (WCHAR *)malloc((cchDisplay+1) * sizeof(WCHAR));

        if (!(pThisBinding->wszBuffer))
        {
            fwprintf(stderr, L"Out of memory!\n");
            exit(-100);
        }

        // Map this buffer to the driver's buffer.   At Fetch time,
        // the driver will fill in this data.  Note that the size is 
        // count of bytes (for Unicode).  All ODBC functions that take
        // SQLPOINTER use count of bytes; all functions that take only
        // strings use count of characters.

        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLBindCol(hStmt,
                    iCol,
                    SQL_C_TCHAR,
                    (SQLPOINTER) pThisBinding->wszBuffer,
                    (cchDisplay + 1) * sizeof(WCHAR),
                    &pThisBinding->indPtr));


        // Now set the display size that we will use to display
        // the data.   Figure out the length of the column name

        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLColAttribute(hStmt,
                    iCol,
                    SQL_DESC_NAME,
                    NULL,
                    0,
                    &cchColumnNameLength,
                    NULL));

        pThisBinding->cDisplaySize = max((SQLSMALLINT)cchDisplay, cchColumnNameLength);
        if (pThisBinding->cDisplaySize < NULL_SIZE)
            pThisBinding->cDisplaySize = NULL_SIZE;

        *pDisplay += pThisBinding->cDisplaySize + DISPLAY_FORMAT_EXTRA;

    }

    return;

Exit:

    exit(-1);

    return;
}


/************************************************************************
/* DisplayTitles: print the titles of all the columns and set the 
/*                shell window's width
/*
/* Parameters:
/*      hStmt          Statement handle
/*      cDisplaySize   Total display size
/*      pBinding        list of binding information
/************************************************************************/

void DisplayTitles(HSTMT     hStmt,
                   DWORD     cDisplaySize,
                   BINDING   *pBinding)
{
    WCHAR           wszTitle[DISPLAY_MAX];
    SQLSMALLINT     iCol = 1;

    SetConsole(cDisplaySize+2, TRUE);

    for (; pBinding; pBinding = pBinding->sNext)
    {
        TRYODBC(hStmt,
                SQL_HANDLE_STMT,
                SQLColAttribute(hStmt,
                    iCol++,
                    SQL_DESC_NAME,
                    wszTitle,
                    sizeof(wszTitle), // Note count of bytes!
                    NULL,
                    NULL));

        wprintf(DISPLAY_FORMAT_C,
                 PIPE,
                 pBinding->cDisplaySize,
                 pBinding->cDisplaySize,
                 wszTitle);
    }

Exit:

    wprintf(L" %c", PIPE);
    SetConsole(cDisplaySize+2, FALSE);
    wprintf(L"\n");

}


/************************************************************************
/* SetConsole: sets console display size and video mode
/*
/*  Parameters
/*      siDisplaySize   Console display size
/*      fInvert         Invert video?
/************************************************************************/

void SetConsole(DWORD dwDisplaySize,
                BOOL  fInvert)
{
    HANDLE                          hConsole;
    CONSOLE_SCREEN_BUFFER_INFO      csbInfo;

    // Reset the console screen buffer size if necessary

    hConsole = GetStdHandle(STD_OUTPUT_HANDLE);

    if (hConsole != INVALID_HANDLE_VALUE)
    {
        if (GetConsoleScreenBufferInfo(hConsole, &csbInfo))
        {
            if (csbInfo.dwSize.X <  (SHORT) dwDisplaySize)
            {
                csbInfo.dwSize.X =  (SHORT) dwDisplaySize;
                SetConsoleScreenBufferSize(hConsole, csbInfo.dwSize);
            }

            gHeight = csbInfo.dwSize.Y;
        }

        if (fInvert)
        {
            SetConsoleTextAttribute(hConsole, (WORD)(csbInfo.wAttributes | BACKGROUND_BLUE));
        }
        else
        {
            SetConsoleTextAttribute(hConsole, (WORD)(csbInfo.wAttributes & ~(BACKGROUND_BLUE)));
        }
    }
}


/************************************************************************
/* HandleDiagnosticRecord : display error/warning information
/*
/* Parameters:
/*      hHandle     ODBC handle
/*      hType       Type of handle (HANDLE_STMT, HANDLE_ENV, HANDLE_DBC)
/*      RetCode     Return code of failing command
/************************************************************************/

void HandleDiagnosticRecord (SQLHANDLE      hHandle,    
                             SQLSMALLINT    hType,  
                             RETCODE        RetCode)
{
    SQLSMALLINT iRec = 0;
    SQLINTEGER  iError;
    WCHAR       wszMessage[1000];
    WCHAR       wszState[SQL_SQLSTATE_SIZE+1];


    if (RetCode == SQL_INVALID_HANDLE)
    {
        fwprintf(stderr, L"Invalid handle!\n");
        return;
    }

    while (SQLGetDiagRec(hType,
                         hHandle,
                         ++iRec,
                         wszState,
                         &iError,
                         wszMessage,
                         (SQLSMALLINT)(sizeof(wszMessage) / sizeof(WCHAR)),
                         (SQLSMALLINT *)NULL) == SQL_SUCCESS)
    {
        // Hide data truncated..
        if (wcsncmp(wszState, L"01004", 5))
        {
            fwprintf(stderr, L"[%5.5s] %s (%d)\n", wszState, wszMessage, iError);
        }
    }

}
