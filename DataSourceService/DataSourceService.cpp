#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#include <stdlib.h>
#include <winsock.h>
#include <TlHelp32.h>
#include <sal.h>
#include <thread>
#include <mutex>

#define DEBUG 0
#define DEFAULT_LISTEN_PORT 5353
#define REG_KEY_DIR TEXT("SOFTWARE\\VehicleLocation")

std::mutex odbc_mutex;
int global_MaxID = 0;

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
									goto Exit; \
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

static bool socket_listen(SOCKET *ListenSocket, const char* ListenAddress, u_short port)
{
	//----------------------
    // Initialize Winsock
    WSADATA wsaData;
    int iResult = 0;
	struct sockaddr_in service;

    iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != NO_ERROR) {
        wprintf(L"WSAStartup() failed with error: %d\n", iResult);
        return false;
    }
    //----------------------
    // Create a SOCKET for listening for incoming connection requests.
    *ListenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (*ListenSocket == INVALID_SOCKET) {
        wprintf(L"socket function failed with error: %ld\n", WSAGetLastError());
        WSACleanup();
        return false;
    }
    //----------------------
    // The sockaddr_in structure specifies the address family,
    // IP address, and port for the socket that is being bound.
    service.sin_family = AF_INET;
    service.sin_addr.s_addr = inet_addr(ListenAddress);
    service.sin_port = htons(port);

    iResult = bind(*ListenSocket, (SOCKADDR *) & service, sizeof (service));
    if (iResult == SOCKET_ERROR) {
        wprintf(L"bind function failed with error %d\n", WSAGetLastError());
        iResult = closesocket(*ListenSocket);
        if (iResult == SOCKET_ERROR)
            wprintf(L"closesocket function failed with error %d\n", WSAGetLastError());
        WSACleanup();
        return false;
    }
    //----------------------
    // Listen for incoming connection requests 
    // on the created socket
    if (listen(*ListenSocket, SOMAXCONN) == SOCKET_ERROR)
        wprintf(L"listen function failed with error: %d\n", WSAGetLastError());

    wprintf(L"Listening on port %d...\n", port);
	
	return true;
}

static bool socket_close(SOCKET socket)
{
    int iResult = closesocket(socket);
    if (iResult == SOCKET_ERROR) {
        wprintf(L"closesocket function failed with error %d\n", WSAGetLastError());
        return false;
    }
    return true;
}

typedef struct {
	UCHAR serial[5];
	UCHAR time[3];
	UCHAR date[3];
	UCHAR latitude[4];
	UCHAR reserved1;
	UCHAR longitude[5];
	UCHAR speed_direction[3];
	UCHAR vehicle_status[4];
	UCHAR usr_alarm_flag[1];
	UCHAR reserved2;
	UCHAR record_no;
} RecvRecord;

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

static RETCODE QueryMaxID(SQLHSTMT hStmt)
{
	SQLSMALLINT sNumResults;
    TRYODBC(hStmt,
        SQL_HANDLE_STMT,
        SQLNumResultCols(hStmt, &sNumResults));
	if (sNumResults > 0) {
		if (SQLFetch(hStmt) == SQL_SUCCESS) {
			long id;
			if (SQLGetData(hStmt, 1, SQL_C_LONG, &id, sizeof(id), NULL) != SQL_SUCCESS) {
				global_MaxID = 0;
				fprintf(stderr, "Info: working on empty database\n");
				return SQL_SUCCESS;
			}
			if (id < 0) {
				fprintf(stderr, "Error: Max record ID from database less than zero\n");
				return SQL_ERROR;
			}
			global_MaxID = id;
			fprintf(stderr, "Info: Current Max record ID is %d\n", global_MaxID);
			return SQL_SUCCESS;
		}
	}

Exit:
	return SQL_ERROR;
}

static RETCODE sql_query_wrapper(SQLHSTMT hStmt, WCHAR *wszInput, RETCODE(*callback)(SQLHSTMT))
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
			if (callback) {
				RetCode = callback(hStmt);
				goto Exit;
			}
#if DEBUG
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
#endif
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

typedef struct {
	int id;
	char serial[10];
	char time[6];
	char date[6];
	double latitude;
	double longitude;
	int speed;
	int direction;
	char vehicle_status[8];
	char usr_alarm_flag[2];
} ParsedRecord;

#define HEX2CHAR(x) ((x) < 10 ? (x) + '0' : (x) - 10 + 'A')
static void Binary2HexChar(char* hex, UCHAR* binary, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		*hex++ = HEX2CHAR(binary[i] >> 4);
		*hex++ = HEX2CHAR(binary[i] & 0xF);
	}
}

#define HEXBYTE2INT(x) (((x) >> 4) * 10 + (x) & 0xF)
static long Hex2Decimal(char *hex, size_t length)
{
	int result = 0;
	for (size_t i = 0; i < length; i++) {
		result *= 10;
		result += hex[i] - '0';
	}
	return result;
}
static double Latitude2Degree(UCHAR* binary)
{
	char latitude[8];
	Binary2HexChar(latitude, binary, 4);
	double x = Hex2Decimal(latitude, 2);
	x += (Hex2Decimal(&latitude[2], 6) * 0.0001) / 60.0;
	return x;
}
static double Longitude2Degree(UCHAR* binary)
{
	char longitude[10];
	Binary2HexChar(longitude, binary, 5);
	double x = Hex2Decimal(longitude, 3);
	x += (Hex2Decimal(&longitude[3], 6) * 0.0001) / 60.0;
	return x;
}

#define MAX_SQLLEN 1024
static void handle_record(RecvRecord *r, SQLHSTMT hStmt)
{
	odbc_mutex.lock();

	ParsedRecord pr;
	pr.id = ++global_MaxID;
	Binary2HexChar(pr.serial, r->serial, sizeof(r->serial));
	Binary2HexChar(pr.time, r->time, sizeof(r->time));
	Binary2HexChar(pr.date, r->date, sizeof(r->date));
	pr.latitude = Latitude2Degree(r->latitude);
	pr.longitude = Longitude2Degree(r->longitude);
	if ((r->longitude[4] & 0xF) == 0) // magic flag for E/W
		pr.longitude = -pr.longitude;
	if ((r->longitude[4] & 0x7) == 0) // magic flag for N/S
		pr.latitude = -pr.latitude;
	char speed_dir[sizeof(r->speed_direction) * 2];
	Binary2HexChar(speed_dir, r->speed_direction, sizeof(r->speed_direction));
	pr.speed = Hex2Decimal(speed_dir, 3);
	pr.direction = Hex2Decimal(speed_dir + 3, 3);
	Binary2HexChar(pr.vehicle_status, r->vehicle_status, sizeof(r->vehicle_status));
	Binary2HexChar(pr.usr_alarm_flag, r->usr_alarm_flag, sizeof(r->usr_alarm_flag));

	fprintf(stderr, "Raw record: ");
	for (int i = 0; i < sizeof(RecvRecord); i++) {
		fprintf(stderr, "%x%x ", (*((BYTE*)r + i) >> 4), (*((BYTE*)r + i) & 0xF));
	}
	fprintf(stderr, "\n");
	char sql[MAX_SQLLEN] = {0};
	sprintf_s(sql, "INSERT INTO location"
		" (id, serial, time, date, latitude, longitude, speed, direction, vehicle_status, usr_alarm_flag)"
		" VALUES (%d,'%.10s','%.6s','%.6s',%lf,%lf,%d,%d,'%.8s','%.2s')",
		pr.id, pr.serial, pr.time, pr.date, pr.latitude, pr.longitude, pr.speed, pr.direction, pr.vehicle_status, pr.usr_alarm_flag);
	fprintf(stderr, "SQL: %s\n", sql);

	WCHAR wsql[MAX_SQLLEN] = {0};
	MultiByteToWideChar(CP_ACP, 0, sql, strlen(sql), wsql, MAX_SQLLEN);

	sql_query_wrapper(hStmt, wsql, NULL);
	odbc_mutex.unlock();
}

DWORD NeverHideWindow;

static bool run_connection(SOCKET ClientSocket, SQLHSTMT hStmt)
{
	bool retval;
	if (!NeverHideWindow)
		ShowWindow(GetConsoleWindow(), SW_HIDE);

	// Receive until the peer shuts down the connection
	while (1) {
		char c;
		int iResult = recv(ClientSocket, &c, 1, 0);
		if (iResult == 1) {
			if (c == '$') { // match record header
				RecvRecord r;
				iResult = recvn(ClientSocket, (char *)&r, sizeof(RecvRecord));
				if (iResult == sizeof(RecvRecord))
					handle_record(&r, hStmt);
			}
		}
		// the following line cannot be "else if"
		if (iResult == 0) {
			printf("Connection closing...\n");
			retval = true;
			goto Exit;
		}
		else if (iResult < 0) {
			printf("recv failed (%d), closing socket %d\n", WSAGetLastError(), ClientSocket);
			retval = false;
			goto Exit;
		}
	}
Exit:
	socket_close(ClientSocket);
	if (!NeverHideWindow)
		ShowWindow(GetConsoleWindow(), SW_SHOW);
	return retval;
}

static void socket_accept(SOCKET ListenSocket, SQLHSTMT hStmt)
{
	while (1) {
		SOCKET ClientSocket = INVALID_SOCKET;

		ClientSocket = accept(ListenSocket, NULL, NULL);
		if (ClientSocket == INVALID_SOCKET) {
			printf("socket accept failed: %d\n", WSAGetLastError());
			continue;
		}
		printf("Client %d connected\n", ClientSocket);

		std::thread *worker = new std::thread(run_connection, ClientSocket, hStmt);
		worker->detach();
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

int __cdecl wmain(void)
{
    SQLHENV     hEnv = NULL;
    SQLHDBC     hDbc = NULL;
    SQLHSTMT    hStmt = NULL;
	WCHAR       *pwszConnStr = new WCHAR[SQL_MAX_LENGTH];

	if (isMyselfRunning()) {
		error(L"Another instance of this service is already running!");
		goto Exit;
	}

    // Allocate an environment

    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv) == SQL_ERROR)
    {
        fwprintf(stderr, L"Unable to allocate an environment handle\n");
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

	WCHAR fullConnStr[MAX_SQLLEN] = {0};
    TRYODBC(hDbc,
        SQL_HANDLE_DBC,
        SQLDriverConnect(hDbc,
                         NULL,
                         pwszConnStr,
                         SQL_NTS,
                         fullConnStr,
						 MAX_SQLLEN,
                         NULL,
                         SQL_DRIVER_COMPLETE));

    fwprintf(stderr, L"Connected!\n");
	fwprintf(stderr, L"%s\n", fullConnStr);

	TRYODBC(hDbc,
		SQL_HANDLE_DBC,
		SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));

	if (SQL_ERROR == sql_query_wrapper(hStmt, L"USE VehicleLocation", NULL)) {
		fprintf(stderr, "Database does not exist, exiting\n");
		goto Exit;
	}

	if (SQL_ERROR == sql_query_wrapper(hStmt, L"SELECT MAX(id) FROM location", QueryMaxID)) {
		fprintf(stderr, "Database table corrupted, exiting\n");
		goto Exit;
	}

	SOCKET ListenSocket;
	u_short listen_port = DEFAULT_LISTEN_PORT;
	DWORD w_portno;
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"ListenPort", 0, &regKeyType, (LPBYTE)&w_portno, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD) {
		fprintf(stderr, "Registry key ListenPort does not exist [errno %d], using default: %d\n", iResult, listen_port);
	} else {
		listen_port = (USHORT)w_portno;
	}
	
	length = sizeof(DWORD);
	iResult = RegQueryValueEx(hKey, L"NeverHideWindow", 0, &regKeyType, (LPBYTE)&NeverHideWindow, &length);
	if (iResult != ERROR_SUCCESS || regKeyType != REG_DWORD)
		NeverHideWindow = 0;
	
	if (!socket_listen(&ListenSocket, "0.0.0.0", listen_port))
		goto Exit;

	socket_accept(ListenSocket, hStmt);

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

    wprintf(L"\nDisconnected to database.");
	MessageBox(GetDesktopWindow(), L"See console window for Error", L"Error", 0);
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
