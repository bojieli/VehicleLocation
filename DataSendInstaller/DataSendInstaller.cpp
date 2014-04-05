#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#include <stdlib.h>
#include <sal.h>
#include <WinSock.h>

#define DEBUG 0
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

#define INSTALL_DIR "%ProgramFiles%\\VehicleLocation"
#define EXECUTABLE "DataSendService.exe"
#define SERVICE_NAME "VehicleLocationDataSend"
#define EXECUTABLE_PATH INSTALL_DIR "\\" EXECUTABLE

int __cdecl wmain(void)
{
    SQLHENV     hEnv = NULL;
    SQLHDBC     hDbc = NULL;
    SQLHSTMT    hStmt = NULL;

InputIP:
	printf("===================================\n");
	printf("Enter Remote Server IP (example: 127.0.0.1): ");
	char remote_ip_str[100];
	scanf_s("%s", remote_ip_str);
	if (inet_addr(remote_ip_str) == INADDR_NONE) {
		fprintf(stderr, "Invalid IP address!\n");
		goto InputIP;
	}
InputPort:
	printf("===================================\n");
	printf("Enter Remote Port (example: 7200): ");
	char portno[100] = {0};
	scanf_s("%s", portno);
	DWORD w_portno = atoi(portno);
	if (w_portno <= 0 || w_portno >= 65536) {
		fprintf(stderr, "Invalid port!\n");
		goto InputPort;
	}

	int SkipAuthentication = IDNO;
	char remote_auth_token[100] = {0};

	printf("===================================\n");
	int UseUDP = MessageBox(GetDesktopWindow(), L"使用 UDP 连接远程服务器吗？",
		L"Installation Option", MB_ICONQUESTION | MB_YESNO | MB_DEFBUTTON2);
	if (UseUDP == IDYES) {
		goto InputNeverHideWindow;
	}

InputAuthToken:
	printf("===================================\n");
	SkipAuthentication = MessageBox(GetDesktopWindow(), L"是否跳过与远程服务器的认证过程？",
		L"Installation Option", MB_ICONQUESTION | MB_YESNO | MB_DEFBUTTON2);
	if (SkipAuthentication == IDNO) {
		printf("Enter Remote Auth Token: ");
		scanf_s("%s", remote_auth_token);
		if (strlen(remote_auth_token) > 50) {
			fprintf(stderr, "Error: authentication too long (more than 50 characters)\n");
			goto InputAuthToken;
		}
	}

InputNeverHideWindow:
	printf("===================================\n");
	int NeverHideWindow = MessageBox(GetDesktopWindow(), L"是否永远不隐藏对话框？(选 No 在连接建立后隐藏对话框，选 Yes 用于调试)",
		L"Installation Option", MB_ICONQUESTION | MB_YESNO | MB_DEFBUTTON2);

	HKEY hKey;
	int iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, REG_KEY_DIR, 0, NULL, 0, KEY_QUERY_VALUE | KEY_SET_VALUE, NULL, &hKey, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	iResult = RegSetValueExA(hKey, "RemoteIP", 0, REG_SZ, (LPBYTE)remote_ip_str, strlen(remote_ip_str));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	iResult = RegSetValueExA(hKey, "RemotePort", 0, REG_DWORD, (LPBYTE)&w_portno, sizeof(w_portno));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	DWORD dwUseUDP = (UseUDP == IDYES) ? 1 : 0;
	iResult = RegSetValueExA(hKey, "UseUDP", 0, REG_DWORD, (LPBYTE)&dwUseUDP, sizeof(DWORD));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	iResult = RegSetValueExA(hKey, "RemoteAuthToken", 0, REG_SZ, (LPBYTE)&remote_auth_token, strlen(remote_auth_token));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	DWORD dwSkipAuthentication = (SkipAuthentication == IDYES) ? 1 : 0;
	iResult = RegSetValueExA(hKey, "SkipAuthentication", 0, REG_DWORD, (LPBYTE)&dwSkipAuthentication, sizeof(DWORD));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	DWORD dwNeverHideWindow = (NeverHideWindow == IDYES) ? 1 : 0;
	iResult = RegSetValueExA(hKey, "NeverHideWindow", 0, REG_DWORD, (LPBYTE)&dwNeverHideWindow, sizeof(DWORD));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}
	DWORD MaxSentID = 0;
	DWORD regKeyType;
	DWORD length = sizeof(DWORD);
	iResult = RegQueryValueExA(hKey, "MaxSentID", 0, &regKeyType, (LPBYTE)&MaxSentID, &length);
	if (iResult == ERROR_SUCCESS && regKeyType == REG_DWORD && MaxSentID != 0) {
		iResult = MessageBox(GetDesktopWindow(), L"检测到有一些数据已经发送过，是否重新发送这些数据？",
			L"Installation Option", MB_ICONQUESTION | MB_YESNO | MB_DEFBUTTON2);
		if (iResult == IDYES)
			MaxSentID = 0;
	}
	iResult = RegSetValueExA(hKey, "MaxSentID", 0, REG_DWORD, (LPBYTE)&MaxSentID, sizeof(DWORD));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key [errno %d]\n", iResult);
		goto Exit;
	}

	printf("Copying Files...\n");
	system("mkdir \"" INSTALL_DIR "\" >nul"); // don't care whether it is executed successfully
	if (system("copy /Y msvcp110.dll \"" INSTALL_DIR "\\\"")) {
		fprintf(stderr, "Failed to copy runtime library\n");
		goto Exit;
	}
	if (system("copy /Y msvcr110.dll \"" INSTALL_DIR "\\\"")) {
		fprintf(stderr, "Failed to copy runtime library\n");
		goto Exit;
	}
	if (system("copy /Y " EXECUTABLE " \"" INSTALL_DIR "\\\"")) {
		fprintf(stderr, "Failed to copy service executable\n");
		goto Exit;
	}

	/*
	printf("Cleaning and installing service\n");
	system(".\\instsrv.exe " SERVICE_NAME " REMOVE >nul");
	if (system(".\\instsrv.exe " SERVICE_NAME " \"" INSTALL_DIR "\\srvany.exe\"")) {
		fprintf(stderr, "Failed to create Windows Service\n");
		goto Exit;
	}
	HKEY hKeyService;
	iResult = RegCreateKeyExA(HKEY_LOCAL_MACHINE, "SYSTEM\\CurrentControlSet\\Services\\" SERVICE_NAME "\\Parameters", 0, NULL, 0, KEY_SET_VALUE, NULL, &hKeyService, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key for service [errno %d]\n", iResult);
		goto Exit;
	}
	iResult = RegSetValueExA(hKeyService, "Application", 0, REG_SZ, (LPBYTE)EXECUTABLE_PATH, sizeof(EXECUTABLE_PATH)-1);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to set application path for service [errno %d]\n", iResult);
		goto Exit;
	}
	*/

	printf("Configuring Autorun...\n");
	HKEY hKeyAutorun;
	iResult = RegCreateKeyEx(HKEY_LOCAL_MACHINE, L"Software\\Microsoft\\Windows\\CurrentVersion\\Run", 0, NULL, 0, KEY_SET_VALUE, NULL, &hKeyAutorun, NULL);
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to open autorun registry [errno %d]\n", iResult);
		goto Exit;
	}
	iResult = RegSetValueExA(hKeyAutorun, SERVICE_NAME, 0, REG_SZ, (LPBYTE)EXECUTABLE_PATH, strlen(EXECUTABLE_PATH));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to set AutoRun for service [errno %d]\n", iResult);
		goto Exit;
	}

    // Allocate an environment

    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv) == SQL_ERROR)
    {
        fwprintf(stderr, L"Unable to allocate an environment handle\n");
        goto Exit;
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

    // Connect to the driver.  Use the connection string if supplied
    // on the input, otherwise let the driver manager prompt for input.

	WCHAR fullConnStr[SQL_QUERY_SIZE] = {0};
    TRYODBC(hDbc,
        SQL_HANDLE_DBC,
        SQLDriverConnect(hDbc,
						 GetDesktopWindow(),
                         L"",
                         SQL_NTS,
                         fullConnStr,
						 SQL_QUERY_SIZE,
                         NULL,
                         SQL_DRIVER_COMPLETE));

    fwprintf(stderr, L"Connected!\n");
	fwprintf(stderr, L"%s\n", fullConnStr);

	iResult = RegSetValueEx(hKey, L"DBConnString", 0, REG_SZ, (LPBYTE)fullConnStr, sizeof(fullConnStr));
	if (iResult != ERROR_SUCCESS) {
		fprintf(stderr, "Failed to create registry key DBConnString [errno %d]\n", iResult);
		goto Exit;
	}

	TRYODBC(hDbc,
		SQL_HANDLE_DBC,
		SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt));

	// if database does not exist, create it
	if (SQL_ERROR == sql_query_wrapper(hStmt, L"USE VehicleLocation")) {
		if (SQL_ERROR == sql_query_wrapper(hStmt, L"CREATE DATABASE VehicleLocation")) {
			fprintf(stderr, "Failed to create database, exiting\n");
			goto Exit;
		}
		if (SQL_ERROR == sql_query_wrapper(hStmt, L"USE VehicleLocation")) {
			fprintf(stderr, "Database create verification failed, exiting\n");
			goto Exit;
		}
		if (SQL_ERROR == sql_query_wrapper(hStmt,
			L"CREATE TABLE location ("
			L"	id int NOT NULL,"
			L"  serial nchar(10) NOT NULL,"
			L"  date nchar(6) NOT NULL,"
			L"  time nchar(6) NOT NULL,"
			L"  longitude real NOT NULL,"
			L"  latitude real NOT NULL,"
			L"  speed int NOT NULL,"
			L"  direction int NOT NULL,"
			L"  height int,"
			L"  accuracy int,"
			L"  vehicle_status nchar(8),"
			L"  usr_alarm_flag nchar(2),"
			L"  PRIMARY KEY (id)"
			L")"
			)) {
				fprintf(stderr, "Failed to create database table, exiting\n");
				goto Exit;
		}
		if (SQL_ERROR == sql_query_wrapper(hStmt, L"SELECT * FROM location")) {
			fprintf(stderr, "Database table verification failed, exiting\n");
			goto Exit;
		}
		printf("Successfully created database.\n");
	}

	wprintf(L"===== Installation completed. =====\n");

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

	wprintf(L"Now you can close the program. It will automatically close in 30 seconds.\n");	
	Sleep(30000);

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
