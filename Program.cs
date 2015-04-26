#define Azure
//#define WriteToDB
#define WriteToGeoInfo
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Text.RegularExpressions;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;
using System.Data.SqlClient;
namespace UAir_AppLogService
{
    class GeoInfoDecoder
    {
        static Dictionary<string, short> LocationHash = new Dictionary<string, short>();
        static List<string> LocationName = new List<string>();
        const int MaxLine = 10000;
        public int RowNum = 0;
        public int ColNum = 0;
        public double LongitudeStart = 1e100;
        public double LongitudeEnd = 1e100;
        public double LatitudeStart = 1e100;
        public double LatitudeEnd = 1e100;
        public double Xstep = 0;
        public double Ystep = 0;
        public short[][] Matrix=null;
        public GeoInfoDecoder(){
            Matrix=new short[MaxLine][];
            for (int i = 0; i < MaxLine; i++)
                Matrix[i] = new short[MaxLine];
        }
        public void GeoInfoFileInput(string Path)
        {
            /*
            Row 10000 5.0 55.0
            Column 10000 70.0 140.0
            6889 6633 北京市
            6889 6634 北京市
             */
            StreamReader fin = new StreamReader(Path,Encoding.UTF8);
            string line_file = fin.ReadLine();
            int line_counter = 0;
            while (line_file != null)
            {
                line_counter++;
                if (line_counter % 1000000 == 0) Console.WriteLine(line_counter);
                if (line_file.IndexOf("Row") == 0)
                {
                    int pos1 = line_file.IndexOf(' ');
                    int pos2 = line_file.IndexOf(' ', pos1+1);
                    int pos3 = line_file.IndexOf(' ', pos2+1);
                    this.RowNum = Convert.ToInt32(line_file.Substring(pos1 + 1, pos2-pos1-1));
                    this.LatitudeStart = Convert.ToDouble(line_file.Substring(pos2 + 1, pos3 - pos2 - 1));
                    this.LatitudeEnd = Convert.ToDouble(line_file.Substring(pos3 + 1));
                }
                else if (line_file.IndexOf("Column") == 0)
                {
                    int pos1 = line_file.IndexOf(' ');
                    int pos2 = line_file.IndexOf(' ', pos1+1);
                    int pos3 = line_file.IndexOf(' ', pos2+1);
                    this.ColNum = Convert.ToInt32(line_file.Substring(pos1 + 1, pos2 - pos1 - 1));
                    this.LongitudeStart = Convert.ToDouble(line_file.Substring(pos2 + 1, pos3 - pos2 - 1));
                    this.LongitudeEnd = Convert.ToDouble(line_file.Substring(pos3 + 1));
                }
                else
                {
                    int pos1 = line_file.IndexOf(' ');
                    int pos2 = line_file.IndexOf(' ', pos1+1);
                    int now_row = Convert.ToInt32(line_file.Substring(0, pos1));
                    int now_col = Convert.ToInt32(line_file.Substring(pos1 + 1, pos2 - pos1 - 1));
                    string now_str = line_file.Substring(pos2 + 1);
                    if (!LocationHash.ContainsKey(now_str))
                    {
                        LocationHash[now_str] = (short)LocationName.Count;
                        LocationName.Add(now_str);
                        AppLogParser.FocusOutput.WriteLine(now_str);
                    }
                    this.Matrix[now_row][now_col] = (short)(LocationHash[now_str]+1);
                }
                line_file = fin.ReadLine();
            }
            fin.Close();
            Xstep = (LongitudeEnd - LongitudeStart) / ColNum;
            Ystep = (LatitudeEnd - LatitudeStart) / RowNum;
        }
        public string SearchLocation(double longitude, double latitude)
        {
            int now_row = (int)((latitude - LatitudeStart) / Ystep);
            if (now_row < 0) return "NULL";
            if (now_row >= RowNum) return "NULL";
            int now_col = (int)((longitude - LongitudeStart) / Xstep);
            if (now_col < 0) return "NULL";
            if (now_col >= ColNum) return "NULL";
            if(Matrix[now_row][now_col]==0) return "NULL";
            return LocationName[Matrix[now_row][now_col] - 1];
        }
        public string SearchLocation(int code)
        {
            KeyValuePair<double,double> pair=SearchPointByCode(code);
            return SearchLocation(pair.Key, pair.Value);
        }
        public int SearchLocationCode(double longitude, double latitude)
        {
            int now_row = (int)((latitude - LatitudeStart) / Ystep);
            if (now_row < 0) return -1;
            if (now_row >= RowNum) return -1;
            int now_col = (int)((longitude - LongitudeStart) / Xstep);
            if (now_col < 0) return -1;
            if (now_col >= ColNum) return -1;
            if (Matrix[now_row][now_col] == 0) return -1;
            return now_row*ColNum+now_col;
        }
        public KeyValuePair<double, double> SearchPointByCode(int code)
        {
            int now_col = code % ColNum;
            int now_row = code / ColNum;
            return new KeyValuePair<double,double>((now_col+0.5)*Xstep+LongitudeStart,(now_row+0.5)*Ystep+LatitudeStart);
        }
    }
    class AppLogParser
    {
        [DllImport("msvcrt.dll")]
        static extern bool system(string str);
        public Dictionary<string, Object> Report =null;
        public static List<FileInfo> GetAllFiles(DirectoryInfo Dir)
        {
            int open = 0;
            int closed = -1;
            List<FileInfo> ans_list = new List<FileInfo>();
            List<FileSystemInfo> queue = new List<FileSystemInfo>();
            queue.Add(Dir);
            while (open != closed)
            {
                closed++;
                if (queue[closed] is FileInfo)
                    ans_list.Add((FileInfo)queue[closed]);
                else
                {
                    queue.AddRange(((DirectoryInfo)queue[closed]).GetFileSystemInfos());
                    open = queue.Count - 1;
                }

            }
            return ans_list;
        }
        public static string Province_Focus = null;
        public static string City_Focus = null;
        public static StreamWriter FocusOutput = null;
        public static string Accumulated_User_List_Path = null;
        public static string Forbiden_Grid_List_Path = null;
        public static Dictionary<string,HashSet<string>> Accumulated_User_List = null;
        public static Dictionary<string, HashSet<string>> Instant_User_list = null;
        public static HashSet<int> Forbiden_Grid_List = null;
        public static Dictionary<int, int> TotalRequestDictionary = null;
        public static Dictionary<int, int> TotalErrorRequestDictionary = null;
        public static void SocketToLocalDB(string IP, int port, string info)
        {
            Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                s.Connect(IPAddress.Parse(IP), port);
                byte[] bytesSendStr = Encoding.UTF8.GetBytes(info);
                s.Send(bytesSendStr, 0);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString()+"\n"+ex.StackTrace);
            }
            finally
            {
                s.Close();
            }
        }
        public class API_Info_Object
        {
            //GetAQIHistory24HourByLocation
            public int Total_GetAQIHistory24HourByLocation_Counter = 0;
            public int Error_GetAQIHistory24HourByLocation_Counter = 0;
            public int Total_Cig_GetAQIHistory24HourByLocation_Counter = 0;
            public int Error_Cig_GetAQIHistory24HourByLocation_Counter = 0;
            public int Total_IOS_GetAQIHistory24HourByLocation_Counter = 0;
            public int Error_IOS_GetAQIHistory24HourByLocation_Counter = 0;
            public int Total_Other_GetAQIHistory24HourByLocation_Counter = 0;
            public int Error_Other_GetAQIHistory24HourByLocation_Counter = 0;
            //GetAQIPredictionByLocation
            public int Total_GetAQIPredictionByLocation_Counter = 0;
            public int Error_GetAQIPredictionByLocation_Counter = 0;
            public int Total_Cig_GetAQIPredictionByLocation_Counter = 0;
            public int Error_Cig_GetAQIPredictionByLocation_Counter = 0;
            public int Total_IOS_GetAQIPredictionByLocation_Counter = 0;
            public int Error_IOS_GetAQIPredictionByLocation_Counter = 0;
            public int Total_Other_GetAQIPredictionByLocation_Counter = 0;
            public int Error_Other_GetAQIPredictionByLocation_Counter = 0;
            //GetAirInfoByLocation
            public int Total_GetAirInfoByLocation_Counter = 0;
            public int Error_GetAirInfoByLocation_Counter = 0;
            public int Total_Cig_GetAirInfoByLocation_Counter = 0;
            public int Error_Cig_GetAirInfoByLocation_Counter = 0;
            public int Total_IOS_GetAirInfoByLocation_Counter = 0;
            public int Error_IOS_GetAirInfoByLocation_Counter = 0;
            public int Total_Other_GetAirInfoByLocation_Counter = 0;
            public int Error_Other_GetAirInfoByLocation_Counter = 0;
            //User_Active_Accumulated_New_Counter
            public int User_Accumulated_Counter_Hour = 0;
            public int User_New_Counter_Hour = 0;
            public int User_Active_Counter_Hour = 0;
        }
        void User_Active_Accumulated_New_Counting(string province_loc,string city_loc,LogObject json,ref API_Info_Object info_obj)
        {
            try
            {
                if (city_loc.Equals("NULL"))
                {
                    if ((province_loc.IndexOf("市") != -1) || (province_loc.IndexOf("香港") != -1) || (province_loc.IndexOf("澳门") != -1))
                        city_loc = province_loc;
                }
                info_obj.User_Accumulated_Counter_Hour = Accumulated_User_List.ContainsKey(city_loc)?Accumulated_User_List[city_loc].Count:0;
                if (!Accumulated_User_List.ContainsKey(city_loc))
                {
                    Accumulated_User_List[city_loc] = new HashSet<string>();
                }
                if (!Accumulated_User_List[city_loc].Contains(json.Args.ClientID))
                {
                    info_obj.User_New_Counter_Hour++;
                    Accumulated_User_List[city_loc].Add(json.Args.ClientID);
                    info_obj.User_Accumulated_Counter_Hour = Accumulated_User_List[city_loc].Count;
                }
                if (!Instant_User_list.ContainsKey(city_loc))
                {
                    Instant_User_list[city_loc] = new HashSet<string>();
                }
                if (!Instant_User_list[city_loc].Contains(json.Args.ClientID))
                {
                    info_obj.User_Active_Counter_Hour++;
                    Instant_User_list[city_loc].Add(json.Args.ClientID);
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString()+"\n"+ex.StackTrace);
                system("pause");
            }
        }
        bool ErrorAnalysis_GetAQIHistory24HourByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAQIHistory24HourByLocation_Counter"))
            {
                Report["Error_GetAQIHistory24HourByLocation_Counter"] = (int)Report["Error_GetAQIHistory24HourByLocation_Counter"] + 1;
            }
            else Report["Error_GetAQIHistory24HourByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"]=new Dictionary<string,Object>();
            Handle = (Dictionary<string,Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Error_GetAQIHistory24HourByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Error_Cig_GetAQIHistory24HourByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Error_IOS_GetAQIHistory24HourByLocation_Counter++;
            }
            else
            {
                info_obj.Error_Other_GetAQIHistory24HourByLocation_Counter++;
            }                                                                                                                                                                          
            //SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::"+json.Args.Lng+"#"+json.Args.Lat+"#");
            return false;
        }
        bool Analysis_GetAQIHistory24HourByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAQIHistory24HourByLocation_Counter"))
            {
                Report["GetAQIHistory24HourByLocation_Counter"] = (int)Report["GetAQIHistory24HourByLocation_Counter"] + 1;
            }
            else Report["GetAQIHistory24HourByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            int code = CityDecoder.SearchLocationCode((double)json.Args.Lng, (double)json.Args.Lat);
            if (user_type.Equals("cig: ") && (!Forbiden_Grid_List.Contains(code)))
            {
                if (!TotalRequestDictionary.ContainsKey(code)) TotalRequestDictionary[code] = 0;
                TotalRequestDictionary[code] = (int)TotalRequestDictionary[code] + 1;
            }
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Total_GetAQIHistory24HourByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Total_Cig_GetAQIHistory24HourByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Total_IOS_GetAQIHistory24HourByLocation_Counter++;
            }
            else
            {
                info_obj.Total_Other_GetAQIHistory24HourByLocation_Counter++;
            }
            User_Active_Accumulated_New_Counting(province_loc,city_loc, json,ref info_obj);
#if WriteToGeoInfo
            SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::" + json.Args.Lng + "#" + json.Args.Lat+"#");
#endif
            return false;
        }
        void ErrorAnalysis_GetAQIHistoryByStationMS(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAQIHistoryByStationMS_Counter"))
            {
                Report["Error_GetAQIHistoryByStationMS_Counter"] = (int)Report["Error_GetAQIHistoryByStationMS_Counter"] + 1;
            }
            else Report["Error_GetAQIHistoryByStationMS_Counter"] = 1;
        }
        void Analysis_GetAQIHistoryByStationMS(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAQIHistoryByStationMS_Counter"))
            {
                Report["GetAQIHistoryByStationMS_Counter"] = (int)Report["GetAQIHistoryByStationMS_Counter"] + 1;
            }
            else Report["GetAQIHistoryByStationMS_Counter"] = 1;
        }
        bool ErrorAnalysis_GetAQIPredictionByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAQIPredictionByLocation_Counter"))
            {
                Report["Error_GetAQIPredictionByLocation_Counter"] = (int)Report["Error_GetAQIPredictionByLocation_Counter"] + 1;
            }
            else Report["Error_GetAQIPredictionByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Error_GetAQIPredictionByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Error_Cig_GetAQIPredictionByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Error_IOS_GetAQIPredictionByLocation_Counter++;
            }
            else
            {
                info_obj.Error_Other_GetAQIPredictionByLocation_Counter++;
            }                                                           
            //SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::" + json.Args.Lng + "#" + json.Args.Lat+"#");
            if (province_loc.Equals(Province_Focus) && city_loc.Equals(City_Focus)) return true;
            return false;
        }
        bool Analysis_GetAQIPredictionByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAQIPredictionByLocation_Counter"))
            {
                Report["GetAQIPredictionByLocation_Counter"] = (int)Report["GetAQIPredictionByLocation_Counter"] + 1;
            }
            else Report["GetAQIPredictionByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            int code = CityDecoder.SearchLocationCode((double)json.Args.Lng, (double)json.Args.Lat);
            if (user_type.Equals("cig: ") && (!Forbiden_Grid_List.Contains(code)))
            {
                if (!TotalRequestDictionary.ContainsKey(code)) TotalRequestDictionary[code] = 0;
                TotalRequestDictionary[code] = (int)TotalRequestDictionary[code] + 1;
            }
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Total_GetAQIPredictionByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Total_Cig_GetAQIPredictionByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Total_IOS_GetAQIPredictionByLocation_Counter++;
            }
            else
            {
                info_obj.Total_Other_GetAQIPredictionByLocation_Counter++;
            }      
#if WriteToGeoInfo
            SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::" + json.Args.Lng + "#" + json.Args.Lat+"#");
#endif
            User_Active_Accumulated_New_Counting(province_loc,city_loc, json, ref info_obj);
            if (province_loc.Equals(Province_Focus) && city_loc.Equals(City_Focus)) return true;
            return false;
        }
        void ErrorAnalysis_GetAirInfoByCity(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAirInfoByCity_Counter"))
            {
                Report["Error_GetAirInfoByCity_Counter"] = (int)Report["Error_GetAirInfoByCity_Counter"] + 1;
            }
            else Report["Error_GetAirInfoByCity_Counter"] = 1;
        }
        void Analysis_GetAirInfoByCity(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAirInfoByCity_Counter"))
            {
                Report["GetAirInfoByCity_Counter"] = (int)Report["GetAirInfoByCity_Counter"] + 1;
            }
            else Report["GetAirInfoByCity_Counter"] = 1;
        }
        bool ErrorAnalysis_GetAirInfoByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAirInfoByLocation_Counter"))
            {
                Report["Error_GetAirInfoByLocation_Counter"] = (int)Report["Error_GetAirInfoByLocation_Counter"] + 1;
            }
            else Report["Error_GetAirInfoByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Error_GetAirInfoByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Error_Cig_GetAirInfoByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Error_IOS_GetAirInfoByLocation_Counter++;
            }
            else
            {
                info_obj.Error_Other_GetAirInfoByLocation_Counter++;
            }          
            //SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::" + json.Args.Lng + "#" + json.Args.Lat+"#");
            return false;
        }
        bool Analysis_GetAirInfoByLocation(string user_type,DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAirInfoByLocation_Counter"))
            {
                Report["GetAirInfoByLocation_Counter"] = (int)Report["GetAirInfoByLocation_Counter"] + 1;
            }
            else Report["GetAirInfoByLocation_Counter"] = 1;
            //-------------------------------------------------------------------------------------------------------------------------
            int code = CityDecoder.SearchLocationCode((double)json.Args.Lng, (double)json.Args.Lat);
            if (user_type.Equals("cig: ") && (!Forbiden_Grid_List.Contains(code)))
            {
                if (!TotalRequestDictionary.ContainsKey(code)) TotalRequestDictionary[code] = 0;
                TotalRequestDictionary[code] = (int)TotalRequestDictionary[code] + 1;
            }
            //-------------------------------------------------------------------------------------------------------------------------
            string province_loc = ProvinceDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            string city_loc = CityDecoder.SearchLocation((double)json.Args.Lng, (double)json.Args.Lat);
            Dictionary<string, Object> Handle = null;
            if (!Report.ContainsKey("GeoDic"))
                Report["GeoDic"] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Report["GeoDic"];
            if (!Handle.ContainsKey(province_loc))
                Handle[province_loc] = new Dictionary<string, Object>();
            Handle = (Dictionary<string, Object>)Handle[province_loc];
            if (!Handle.ContainsKey(city_loc))
                Handle[city_loc] = new API_Info_Object();
            API_Info_Object info_obj = (API_Info_Object)Handle[city_loc];
            info_obj.Total_GetAirInfoByLocation_Counter++;
            if (user_type.Equals("cig: "))
            {
                info_obj.Total_Cig_GetAirInfoByLocation_Counter++;
            }
            else if (user_type.Equals("UrWea"))
            {
                info_obj.Total_IOS_GetAirInfoByLocation_Counter++;
            }
            else
            {
                info_obj.Total_Other_GetAirInfoByLocation_Counter++;
            }
            User_Active_Accumulated_New_Counting(province_loc,city_loc, json, ref info_obj);
#if WriteToGeoInfo
            SocketToLocalDB("127.0.0.1", 1234, "InsertPoint::" + json.Args.Lng + "#" + json.Args.Lat+"#");
#endif
            return false;
        }
        void ErrorAnalysis_GetAirInfoByStationMS(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("Error_GetAirInfoByStationMS_Counter"))
            {
                Report["Error_GetAirInfoByStationMS_Counter"] = (int)Report["Error_GetAirInfoByStationMS_Counter"] + 1;
            }
            else Report["Error_GetAirInfoByStationMS_Counter"] = 1;
        }
        void Analysis_GetAirInfoByStationMS(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetAirInfoByStationMS_Counter"))
            {
                Report["GetAirInfoByStationMS_Counter"] = (int)Report["GetAirInfoByStationMS_Counter"] + 1;
            }
            else Report["GetAirInfoByStationMS_Counter"] = 1;
        }
        void ErrorAnalysis_GetGeoInfo(DateTime Timestamp,LogObject json){
            if (Report.ContainsKey("Error_GetGeoInfo_Counter"))
            {
                Report["Error_GetGeoInfo_Counter"] = (int)Report["Error_GetGeoInfo_Counter"] + 1;
            }
            else Report["Error_GetGeoInfo_Counter"] = 1;
        }
        void Analysis_GetGeoInfo(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetGeoInfo_Counter"))
            {
                Report["GetGeoInfo_Counter"] = (int)Report["GetGeoInfo_Counter"] + 1;
            }
            else Report["GetGeoInfo_Counter"] = 1;
        } 
        void ErrorAnalysis_GetStationAQIByCity(DateTime Timestamp,LogObject json){
            if (Report.ContainsKey("Error_GetStationAQIByCity_Counter"))
            {
                Report["Error_GetStationAQIByCity_Counter"] = (int)Report["Error_GetStationAQIByCity_Counter"] + 1;
            }
            else Report["Error_GetStationAQIByCity_Counter"] = 1;
        }
        void Analysis_GetStationAQIByCity(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetStationAQIByCity_Counter"))
            {
                Report["GetStationAQIByCity_Counter"] = (int)Report["GetStationAQIByCity_Counter"] + 1;
            }
            else Report["GetStationAQIByCity_Counter"] = 1;
        }
        void ErrorAnalysis_GetStationAQIPredictionByCity(DateTime Timestamp,LogObject json)
        {
            if (Report.ContainsKey("Error_GetStationAQIPredictionByCity_Counter"))
            {
                Report["Error_GetStationAQIPredictionByCity_Counter"] = (int)Report["Error_GetStationAQIPredictionByCity_Counter"] + 1;
            }
            else Report["Error_GetStationAQIPredictionByCity_Counter"] = 1;
        }
        void Analysis_GetStationAQIPredictionByCity(DateTime Timestamp, LogObject json)
        {
            if (Report.ContainsKey("GetStationAQIPredictionByCity_Counter"))
            {
                Report["GetStationAQIPredictionByCity_Counter"] = (int)Report["GetStationAQIPredictionByCity_Counter"] + 1;
            }
            else Report["GetStationAQIPredictionByCity_Counter"] = 1;
        }
        [DataContract]
        public class ArgObject
        {
            [DataMember]
            public double? Lat = null;
            [DataMember]
            public double? Lng = null;
            [DataMember]
            public string CityID = null;
            [DataMember]
            public string ClientID = null;
        }
        [DataContract]
        public class LogObject
        {
            [DataMember]
            public string Category = null;
            [DataMember]
            public string Member = null;
            [DataMember]
            public int? ProcessTime = null;
            [DataMember]
            public string IP = null;
            [DataMember]
            public ArgObject Args = null;

        }
        //{""Category"":""API.asmx"",""Member"":""GetAirInfoByLocation"",""Args"":{""Lat"":34.2305063482563,""Lng"":108.831325139583,""ClientID"":""cig: 101_KWmz90yMBc1KNLzrad79G0LJJLc=""},""ProcessTime"":0,""IP"":""117.36.82.91""}
        bool DispatchErrorLog(string Date_str, string Time_str, string level_str, string json_str)
        {
            if (!level_str.Equals("Error")) return false;
            DateTime DateTime_value=DateTime.Parse(Date_str+" "+Time_str);
            LogObject log_obj=new LogObject();
            MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(json_str));
            System.Runtime.Serialization.Json.DataContractJsonSerializer serializer = new System.Runtime.Serialization.Json.DataContractJsonSerializer(log_obj.GetType());
            log_obj = serializer.ReadObject(ms) as LogObject;
            ms.Close();
            string user_type = "other";
            if (log_obj.Args.ClientID != null)
            {
                string ID_str = log_obj.Args.ClientID.Substring(0, 5);
                if ((!ID_str.Equals("UrWea")) && (!ID_str.Equals("cig: "))) ID_str = "other";
                user_type = ID_str;
                ID_str = "Error_Client_" + ID_str + "_" + log_obj.Member;
                if (Report.ContainsKey(ID_str))
                {
                    Report[ID_str] = (int)Report[ID_str] + 1;
                }
                else Report[ID_str] = 1;
            }
            if (user_type.Equals("cig: ") && (log_obj.Member.EndsWith("ByLocation")))
            {
                int code = CityDecoder.SearchLocationCode((double)log_obj.Args.Lng, (double)log_obj.Args.Lat);
                if (!TotalErrorRequestDictionary.ContainsKey(code)) TotalErrorRequestDictionary[code] = 0;
                TotalErrorRequestDictionary[code] = (int)TotalErrorRequestDictionary[code] + 1;
            }
            if(log_obj.Member.Equals("GetAQIHistory24HourByLocation")){
                return ErrorAnalysis_GetAQIHistory24HourByLocation(user_type, DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetAQIHistoryByStationMS")){
                ErrorAnalysis_GetAQIHistoryByStationMS(DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetAQIPredictionByLocation")){
                return ErrorAnalysis_GetAQIPredictionByLocation(user_type,DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetAirInfoByCity")){
                ErrorAnalysis_GetAirInfoByCity(DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetAirInfoByLocation")){
                return ErrorAnalysis_GetAirInfoByLocation(user_type,DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetAirInfoByStationMS")){
                ErrorAnalysis_GetAirInfoByStationMS(DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetGeoInfo")){
                ErrorAnalysis_GetGeoInfo(DateTime_value, log_obj);
            }else if(log_obj.Member.Equals("GetStationAQIByCity")){
                ErrorAnalysis_GetStationAQIByCity(DateTime_value, log_obj);
            }else if (log_obj.Member.Equals("GetStationAQIPredictionByCity")){
                ErrorAnalysis_GetStationAQIPredictionByCity(DateTime_value, log_obj);
            }else if(log_obj.Member!=null)
            {
                if (Report.ContainsKey("Error_Omission_Counter"))
                {
                    Report["Error_Omission_Counter"] = (int)Report["Error_Omission_Counter"] + 1;
                }
                else Report["Error_Omission_Counter"] = 1;
                if (Report.ContainsKey("Error_Omission_" + log_obj.Member))
                {
                    Report["Error_Omission_" + log_obj.Member] = (int)Report["Error_Omission_" + log_obj.Member] + 1;
                }
                else Report["Error_Omission_" + log_obj.Member] = 1;
            }
            return false;
        }
        bool DispatchLog(string Date_str, string Time_str, string level_str, string json_str)
        {
            if (level_str.Equals("Error")) return false;
            DateTime DateTime_value = DateTime.Parse(Date_str + " " + Time_str);
            LogObject log_obj = new LogObject();
            MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(json_str));
            System.Runtime.Serialization.Json.DataContractJsonSerializer serializer = new System.Runtime.Serialization.Json.DataContractJsonSerializer(log_obj.GetType());
            log_obj = serializer.ReadObject(ms) as LogObject;
            ms.Close();
            string user_type="other";
            if (log_obj.Args.ClientID != null)
            {
                string ID_str = log_obj.Args.ClientID.Substring(0, 5);
                if((!ID_str.Equals("UrWea"))&&(!ID_str.Equals("cig: "))) ID_str="other";
                user_type = ID_str;
                ID_str = "Client_" + ID_str + "_" + log_obj.Member;
                if (Report.ContainsKey(ID_str))
                {
                    Report[ID_str] = (int)Report[ID_str] + 1;
                }
                else Report[ID_str] = 1;
            }
            if (log_obj.Member.Equals("GetAQIHistory24HourByLocation"))
            {
                return Analysis_GetAQIHistory24HourByLocation(user_type,DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetAQIHistoryByStationMS"))
            {
                Analysis_GetAQIHistoryByStationMS(DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetAQIPredictionByLocation"))
            {
                return Analysis_GetAQIPredictionByLocation(user_type,DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetAirInfoByCity"))
            {
                Analysis_GetAirInfoByCity(DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetAirInfoByLocation"))
            {
                return Analysis_GetAirInfoByLocation(user_type,DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetAirInfoByStationMS"))
            {
                Analysis_GetAirInfoByStationMS(DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetGeoInfo"))
            {
                Analysis_GetGeoInfo(DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetStationAQIByCity"))
            {
                Analysis_GetStationAQIByCity(DateTime_value, log_obj);
            }
            else if (log_obj.Member.Equals("GetStationAQIPredictionByCity"))
            {
                Analysis_GetStationAQIPredictionByCity(DateTime_value, log_obj);
            }
            else if(log_obj.Member!=null)
            {
                if (Report.ContainsKey("Omission_Counter"))
                {
                    Report["Omission_Counter"] = (int)Report["Omission_Counter"] + 1;
                }
                else Report["Omission_Counter"] = 1;
                if (Report.ContainsKey("Omission_" + log_obj.Member))
                {
                    Report["Omission_" + log_obj.Member] = (int)Report["Omission_" + log_obj.Member] + 1;
                }
                else Report["Omission_" + log_obj.Member] = 1;
            }
            return false;
        }
        public void DictionaryToDB(DateTime Timestamp,string title)
        {   
#if Azure     
            string Conn_str="Data Source=tcp:dy8lnjfo1r.database.windows.net,1433;Initial Catalog=UAirDBv3Test;User ID=adm@dy8lnjfo1r;Password=abcd1234!;";
#else
            string Conn_str="Data Source=127.0.0.1;Initial Catalog=AppLogAnalysis;User Id=DBUser;Password=DBUser;";
#endif
            using (SqlConnection conn = new SqlConnection(Conn_str))
            {
                try
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = conn;
                    int Total_Counter;
                    int Failure_Counter;
                    int Total_Cig_Counter;
                    int Failure_Cig_Counter;
                    int Total_IOS_Counter;
                    int Failure_IOS_Counter;
                    int Total_Other_Counter;
                    int Failure_Other_Counter;
                    if (Report.ContainsKey(title+"_Counter"))
                    {
                        Total_Counter = (int)Report[title+"_Counter"];
                        Failure_Counter = Report.ContainsKey("Error_"+title+"_Counter") ? (int)Report["Error_"+title+"_Counter"] : 0;
                        Total_Cig_Counter = Report.ContainsKey("Client_cig: _" + title) ? (int)Report["Client_cig: _" + title] : 0;
                        Total_IOS_Counter = Report.ContainsKey("Client_UrWea_" + title) ? (int)Report["Client_UrWea_" + title] : 0;
                        Total_Other_Counter = Report.ContainsKey("Client_other_" + title) ? (int)Report["Client_other_" + title] : 0;
                        Failure_Cig_Counter = Report.ContainsKey("Error_Client_cig: _"+title) ? (int)Report["Error_Client_cig: _"+title] : 0;
                        Failure_IOS_Counter = Report.ContainsKey("Error_Client_UrWea_"+title) ? (int)Report["Error_Client_UrWea_"+title] : 0;
                        Failure_Other_Counter = Report.ContainsKey("Error_Client_other_"+title) ? (int)Report["Error_Client_other_"+title] : 0;
                        cmd.CommandText = "Exec Insert_API_Performance '" + Timestamp + "','"+title+"',"
                            + Total_Counter + "," + Failure_Counter + "," + Total_Cig_Counter + "," +Failure_Cig_Counter+
                            ","+ Total_IOS_Counter +","+Failure_IOS_Counter+ "," + Total_Other_Counter + ","+Failure_Other_Counter+";";
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex.ToString()+"\n"+ex.StackTrace);
                }
                finally
                {
                    conn.Close();
                }
            }
        }
        public void GeoDictionaryToDB(DateTime Timestamp)
        {
            if(!Report.ContainsKey("GeoDic")) return;
#if Azure
            string Conn_str = "Data Source=tcp:dy8lnjfo1r.database.windows.net,1433;Initial Catalog=UAirDBv3Test;User ID=adm@dy8lnjfo1r;Password=abcd1234!;";
#else
            string Conn_str="Data Source=127.0.0.1;Initial Catalog=AppLogAnalysis;User Id=DBUser;Password=DBUser;";
#endif
            using (SqlConnection conn = new SqlConnection(Conn_str))
            {
                try
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = conn;
                    int Total_Counter;
                    int Failure_Counter;
                    int Total_Cig_Counter;
                    int Failure_Cig_Counter;
                    int Total_IOS_Counter;
                    int Failure_IOS_Counter;
                    int Total_Other_Counter;
                    int Failure_Other_Counter;
                    Dictionary<string,Object> Handle_i=(Dictionary<string,Object>)Report["GeoDic"];
                    foreach (string province_loc in Handle_i.Keys)
                    {
                        Dictionary<string, Object> Handle_j = (Dictionary<string, Object>)Handle_i[province_loc];
                        foreach (string city_loc in Handle_j.Keys)
                        {
                            API_Info_Object Handle_k = (API_Info_Object)Handle_j[city_loc];
                            //Handle_k.Total_GetAirInfoByLocation_Counter;
                            Total_Counter = Handle_k.Total_GetAirInfoByLocation_Counter;
                            if (Total_Counter > 0)
                            {
                                Failure_Counter = Handle_k.Error_GetAirInfoByLocation_Counter;
                                Total_Cig_Counter = Handle_k.Total_Cig_GetAirInfoByLocation_Counter;
                                Failure_Cig_Counter = Handle_k.Error_Cig_GetAirInfoByLocation_Counter;
                                Total_IOS_Counter = Handle_k.Total_IOS_GetAirInfoByLocation_Counter;
                                Failure_IOS_Counter = Handle_k.Error_IOS_GetAirInfoByLocation_Counter;
                                Total_Other_Counter = Handle_k.Total_Other_GetAirInfoByLocation_Counter;
                                Failure_Other_Counter = Handle_k.Error_Other_GetAirInfoByLocation_Counter;
                                cmd.CommandText = "Exec Insert_Geo_API_Performance '" + Timestamp + "','GetAirInfoByLocation',N'" + province_loc + "',N'" + city_loc + "',"
                                + Total_Counter + "," + Failure_Counter + "," + Total_Cig_Counter + "," + Failure_Cig_Counter +
                                "," + Total_IOS_Counter + "," + Failure_IOS_Counter + "," + Total_Other_Counter + "," + Failure_Other_Counter + ";";
                                cmd.ExecuteNonQuery();
                            }
                            //Handle_k.Total_GetAQIHistory24HourByLocation_Counter;
                            Total_Counter = Handle_k.Total_GetAQIHistory24HourByLocation_Counter;
                            if (Total_Counter > 0)
                            {
                                Failure_Counter = Handle_k.Error_GetAQIHistory24HourByLocation_Counter;
                                Total_Cig_Counter = Handle_k.Total_Cig_GetAQIHistory24HourByLocation_Counter;
                                Failure_Cig_Counter = Handle_k.Error_Cig_GetAQIHistory24HourByLocation_Counter;
                                Total_IOS_Counter = Handle_k.Total_IOS_GetAQIHistory24HourByLocation_Counter;
                                Failure_IOS_Counter = Handle_k.Error_IOS_GetAQIHistory24HourByLocation_Counter;
                                Total_Other_Counter = Handle_k.Total_Other_GetAQIHistory24HourByLocation_Counter;
                                Failure_Other_Counter = Handle_k.Error_Other_GetAQIHistory24HourByLocation_Counter;
                                cmd.CommandText = "Exec Insert_Geo_API_Performance '" + Timestamp + "','GetAQIHistory24HourByLocation',N'" + province_loc + "',N'" + city_loc + "',"
                                + Total_Counter + "," + Failure_Counter + "," + Total_Cig_Counter + "," + Failure_Cig_Counter +
                                "," + Total_IOS_Counter + "," + Failure_IOS_Counter + "," + Total_Other_Counter + "," + Failure_Other_Counter + ";";
                                cmd.ExecuteNonQuery();
                            }
                            //Handle_k.Total_GetAQIPredictionByLocation_Counter;
                            Total_Counter = Handle_k.Total_GetAQIPredictionByLocation_Counter;
                            if (Total_Counter > 0)
                            {
                                Failure_Counter = Handle_k.Error_GetAQIPredictionByLocation_Counter;
                                Total_Cig_Counter = Handle_k.Total_Cig_GetAQIPredictionByLocation_Counter;
                                Failure_Cig_Counter = Handle_k.Error_Cig_GetAQIPredictionByLocation_Counter;
                                Total_IOS_Counter = Handle_k.Total_IOS_GetAQIPredictionByLocation_Counter;
                                Failure_IOS_Counter = Handle_k.Error_IOS_GetAQIPredictionByLocation_Counter;
                                Total_Other_Counter = Handle_k.Total_Other_GetAQIPredictionByLocation_Counter;
                                Failure_Other_Counter = Handle_k.Error_Other_GetAQIPredictionByLocation_Counter;
                                cmd.CommandText = "Exec Insert_Geo_API_Performance '" + Timestamp + "','GetAQIPredictionByLocation',N'" + province_loc + "',N'" + city_loc + "',"
                                + Total_Counter + "," + Failure_Counter + "," + Total_Cig_Counter + "," + Failure_Cig_Counter +
                                "," + Total_IOS_Counter + "," + Failure_IOS_Counter + "," + Total_Other_Counter + "," + Failure_Other_Counter + ";";
                                cmd.ExecuteNonQuery();
                            }
                            //Active,Accumulated,New;
                            cmd.CommandText = "Exec Insert_User_Distribution_Accumulated_New_Counter '" + Timestamp + "',N'"
                                + city_loc + "'," + Handle_k.User_New_Counter_Hour + "," + Handle_k.User_Accumulated_Counter_Hour + ";";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "Exec Insert_User_Distribution_Active_Counter '" + Timestamp + "',N'" + city_loc + "','Hour'," + Handle_k.User_Active_Counter_Hour + ";";
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex.ToString()+"\n"+ex.StackTrace);
                    system("pause");
                }
                finally
                {
                    conn.Close();
                }
            }
        }
        public void Parse(string Prefix,DateTime Timestamp)
        {
            Report=Parse(Prefix + "\\" + Timestamp.ToString("yyyy\\\\MM\\\\dd\\\\HH"));
            if (Report == null) return;
#if WriteToDB
            DictionaryToDB(Timestamp, "GetAQIHistory24HourByLocation");
            DictionaryToDB(Timestamp, "GetAQIHistoryByStationMS");
            DictionaryToDB(Timestamp, "GetAQIPredictionByLocation");
            DictionaryToDB(Timestamp, "GetAirInfoByCity");
            DictionaryToDB(Timestamp, "GetAirInfoByLocation");
            DictionaryToDB(Timestamp, "GetAirInfoByStationMS");
            DictionaryToDB(Timestamp, "GetGeoInfo");
            DictionaryToDB(Timestamp, "GetStationAQIByCity");
            DictionaryToDB(Timestamp, "GetStationAQIPredictionByCity");
            DictionaryToDB(Timestamp, "Ping");
            GeoDictionaryToDB(Timestamp);
#endif
        }
        static UAir_AppLogService.GeoInfoDecoder ProvinceDecoder = null;
        static UAir_AppLogService.GeoInfoDecoder CityDecoder = null;
        public Dictionary<string,Object> Parse(String LogDir)
        {
            if (ProvinceDecoder == null)
            {
                ProvinceDecoder = new GeoInfoDecoder();
                ProvinceDecoder.GeoInfoFileInput("ProvinceGrids[UTF8].txt");
            }
            if (CityDecoder == null)
            {
                CityDecoder = new GeoInfoDecoder();
                CityDecoder.GeoInfoFileInput("CityGrids[UTF8].txt");
            }
            if (Accumulated_User_List == null)
            {
                if (Accumulated_User_List_Path == null) return null;
                StreamReader Accumulated_User_List_Fin = new StreamReader(Accumulated_User_List_Path);
                Accumulated_User_List = new Dictionary<string,HashSet<string>>();
                string fin_str = Accumulated_User_List_Fin.ReadLine();
                while (fin_str != null)
                {
                    int pos = fin_str.IndexOf(' ');
                    string loc_name = fin_str.Substring(0, pos);
                    string user_name = fin_str.Substring(pos + 1);
                    if (!Accumulated_User_List.ContainsKey(loc_name))
                    {
                        Accumulated_User_List[loc_name] = new HashSet<string>();
                    }
                    Accumulated_User_List[loc_name].Add(fin_str);
                    fin_str = Accumulated_User_List_Fin.ReadLine();
                }
                Accumulated_User_List_Fin.Close();
            }
            if (Forbiden_Grid_List == null)
            {
                if (Forbiden_Grid_List_Path == null) return null;
                StreamReader Forbiden_Grid_List_Fin = new StreamReader(Forbiden_Grid_List_Path);
                Forbiden_Grid_List = new HashSet<int>();
                string fin_str = Forbiden_Grid_List_Fin.ReadLine();
                while (fin_str != null)
                {
                    Forbiden_Grid_List.Add(Convert.ToInt32(fin_str));
                    fin_str = Forbiden_Grid_List_Fin.ReadLine();
                }
                Forbiden_Grid_List_Fin.Close();
            }
            Instant_User_list = new Dictionary<string, HashSet<string>>();
            TotalRequestDictionary = new Dictionary<int,int>();
            TotalErrorRequestDictionary = new Dictionary<int,int>();
            if (!Directory.Exists(LogDir)) return null;
            Report = new Dictionary<string, object>();
            DirectoryInfo dir = new DirectoryInfo(LogDir);
            List<FileInfo> files = GetAllFiles(dir);
            string json_str = null;
            string Date_str;
            string Time_str;
            string level_str;
            string applicationName_str;
            string instanceId_str;
            string eventTickCount_str;
            string eventId_str;
            string pid_str;
            string tid_str;
            //SocketToLocalDB("127.0.0.1", 1234, "DeletePointAll::");
            foreach (FileInfo File_item in files)
            {
                Console.Error.WriteLine(File_item.Directory.Parent.Parent.Parent+"\\"+File_item.Directory.Parent.Parent + "\\" + File_item.Directory.Parent + "\\" + File_item.Directory.Name + "\\" + File_item);
                SocketToLocalDB("127.0.0.1", 1234, "ShowTextArea2::" + File_item.Directory.Parent.Parent.Parent + "\\" + File_item.Directory.Parent.Parent + "\\" + File_item.Directory.Parent + "\\" + File_item.Directory.Name + "\\" + File_item);
                StreamReader fin = new StreamReader(File_item.Open(FileMode.Open));
                string fin_str = fin.ReadLine();
                while (fin_str != null)
                {
                    //date,level,applicationName,instanceId,eventTickCount,eventId,pid,tid,message,activityId 
                    //2015-04-06T08:59:59,Information,uair-newdb-test,ae16ed,635639075997145278,0,3908,212,"-1010011-Information-Soap request at API.asmx-{""Category"":""API.asmx"",""Member"":""GetAirInfoByLocation"",""Args"":{""Lat"":34.2305063482563,""Lng"":108.831325139583,""ClientID"":""cig: 101_KWmz90yMBc1KNLzrad79G0LJJLc=""},""ProcessTime"":0,""IP"":""117.36.82.91""}",00000000-0000-0000-6752-0080010000d3
                    if ((fin_str.IndexOf("date,level") != 0) && (fin_str.IndexOf("\"\"Exp\"\"") == -1) && (fin_str.IndexOf("-Information-Site started-{")==-1))
                    {
                        Regex reg = new Regex(@"^(?<date>[0-9|-]+)T(?<time>[0-9|:]+),(?<level>\w+),(?<applicationName>[\w|\d|-]+),(?<instanceId>[\w|\d|-]+),(?<eventTickCount>[\w|\d|-]+),(?<eventId>[\w|\d|-]+),(?<pid>[\w|\d|-]+),(?<tid>[\w|\d|-]+),");
                        Match m = reg.Match(fin_str);
                        if (m.Success)
                        {
                            Date_str = m.Result("${date}");
                            Time_str = m.Result("${time}");
                            level_str = m.Result("${level}");
                            applicationName_str = m.Result("${applicationName}");
                            instanceId_str = m.Result("${instanceId}");
                            eventTickCount_str = m.Result("${eventTickCount}");
                            eventId_str = m.Result("${eventId}");
                            pid_str = m.Result("${pid}");
                            tid_str = m.Result("${tid}");
                            if (!level_str.Equals("Error"))
                            {
                                int pos1 = fin_str.IndexOf("-{");
                                int pos2 = fin_str.IndexOf("}\",");
                                json_str = fin_str.Substring(pos1 + 1, pos2 - pos1);
                                json_str = json_str.Replace("\"\"", "\"");
                                json_str = json_str.Replace("E\\-", "E-");
                                json_str = json_str.Replace(" \\-", "-");
                                json_str = json_str.Replace("\\-", "");
                            }
                            try
                            {
                                if (DispatchLog(Date_str, Time_str, level_str, json_str)||DispatchErrorLog(Date_str, Time_str, level_str, json_str))
                                {
                                    if (FocusOutput != null)
                                    {
                                        FocusOutput.WriteLine(File_item.Directory.Parent.Parent.Parent + "\\" + File_item.Directory.Parent.Parent + "\\" + File_item.Directory.Parent + "\\" + File_item.Directory.Name + "\\" + File_item);
                                        FocusOutput.WriteLine("\nFocus " + Province_Focus + " " + City_Focus + " log\n=========>\n" + fin_str);
                                        fin_str = fin.ReadLine();
                                        while ((fin_str != null) && (!new Regex(@"^(?<date>[0-9|-]+)T(?<time>[0-9|:]+),").Match(fin_str).Success))
                                        {
                                            FocusOutput.WriteLine(fin_str);
                                            fin_str = fin.ReadLine();
                                        }
                                        //system("pause");
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {//skip unjson log
                                Console.WriteLine("\nskip unjson log\n" + fin_str + "\n\n" + json_str + "\n" + ex.ToString()+"\n"+ex.StackTrace);
                            }
                        }
                        else
                        {//skip unReglar log
                            Console.WriteLine("\nskip unReglar log\n=========>" + fin_str);
                            fin_str = fin.ReadLine();
                            while ((fin_str != null) && (!new Regex(@"^(?<date>[0-9|-]+)T(?<time>[0-9|:]+),").Match(fin_str).Success))
                            {
                                Console.WriteLine(fin_str);
                                fin_str = fin.ReadLine();
                            }
                            continue;
                        }
                    }
                    if (fin_str.IndexOf("\"\"Exp\"\"") != -1) Console.WriteLine("\nskip Exception log\n=========>" + fin_str);
                    fin_str = fin.ReadLine();
                    while ((fin_str != null) && (!new Regex(@"^(?<date>[0-9|-]+)T(?<time>[0-9|:]+),").Match(fin_str).Success))
                    {//Skip the exception
                        Console.WriteLine(fin_str);
                        fin_str = fin.ReadLine();
                    }
                }
                fin.Close();
            }
            return Report;
        }
        public static void Output_Accumulated_User_List()
        {
            StreamWriter Accumulated_User_List_Fout = new StreamWriter(Accumulated_User_List_Path);
            foreach (string loc_name in Accumulated_User_List.Keys)
            {
                foreach (string user_name in Accumulated_User_List[loc_name])
                {
                    Accumulated_User_List_Fout.WriteLine(loc_name+" "+user_name);
                }
            }
            Accumulated_User_List_Fout.Close();
        }
        public void Academic_Parser(String LogDir)
        {
            Report=Parse(LogDir);
            var utf8WithBom = new System.Text.UTF8Encoding(true);
            StreamWriter fout = new StreamWriter("Academic.csv", false, utf8WithBom);
            fout.WriteLine("longitude,latitude,code,counter,city");
            foreach (int code_iter in TotalRequestDictionary.Keys)
            {
                int counter = TotalRequestDictionary[code_iter];
                string loc_name = CityDecoder.SearchLocation(code_iter);
                KeyValuePair<double, double> pair = CityDecoder.SearchPointByCode(code_iter);
                fout.WriteLine(pair.Key + "," + pair.Value + "," + code_iter + "," + counter + "," + loc_name);
            }
            /* Output Forbiden List
            Dictionary<string, KeyValuePair<int, int>> CenterTable = new Dictionary<string, KeyValuePair<int, int>>();
            foreach(int code_iter in TotalRequestDictionary.Keys){
                int counter = TotalRequestDictionary[code_iter];
                string loc_name = CityDecoder.SearchLocation(code_iter);
                if (!CenterTable.ContainsKey(loc_name)) CenterTable[loc_name] = new KeyValuePair<int, int>(code_iter, counter);
                else if (CenterTable[loc_name].Value < counter) CenterTable[loc_name] = new KeyValuePair<int, int>(code_iter, counter);
            }
            foreach (string loc_name in CenterTable.Keys)
            {
                int code = CenterTable[loc_name].Key;
                int counter = CenterTable[loc_name].Value;
                KeyValuePair<double, double> pair = CityDecoder.SearchPointByCode(code);
                fout.WriteLine(pair.Key + "," + pair.Value + "," + code + "," + counter + "," + loc_name);
            }
            */
            fout.Close();
        }
        static void Main(string[] args)
        {
            DateTime st_time=DateTime.Parse("2015-03-01 00:00:00");
            DateTime en_time = DateTime.Parse("2015-04-16 10:00:00");
            AppLogParser.FocusOutput = new StreamWriter("Focus.txt");
            AppLogParser.Accumulated_User_List_Path = "Accumulated_User_List.txt";
            AppLogParser.Forbiden_Grid_List_Path = "Forbiden.txt";
            /*
            for (; st_time < en_time; st_time = st_time.AddHours(1))
            {
                new AppLogParser().Parse("LogInput", st_time);
            }
             */
            new AppLogParser().Academic_Parser("LogInput");
            AppLogParser.FocusOutput.Close();
            AppLogParser.Output_Accumulated_User_List();
            return;
        }
    }
}
