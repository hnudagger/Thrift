namespace java com.ifeng.uimg.Interface
struct Recommend {
      1:string uid;
      2:string first_in;
      3:string general_in;
      4:string general_loc;
      5:list<string> ip;
      6:string iphone;
      7:string last_in;
      8:list<string> last_t1;
      9:list<string> last_t2;
      10:string last_t2_sourceSims;
      11:list<string> last_t3;
      12:string last_ub_sourceSims;
      13:string last_ucombineTag;
      14:string last_utime;
      15:list<string> lastitems;
      16:bool likevideo;
      17:string loc;
      18:list<string> recent_t1;
      19:list<string> recent_t2;
      20:list<string> recent_t3;
      21:string recent_ucombineTag;
      22:list<string> s;
      23:list<string> t1;
      24:list<string> t2;
      25:list<string> t3;
      26:string ua;
      27:string ua_r;
      28:string ua_v;
      29:string ucombineTag;
      30:string umos;
      31:string umt;
      32:string user_schannel;
      33:string utime;
      34:string uver;
}

struct UserPage{
    1:string userId;
    2:list<string> pageIds;
}

service UimgService {
    void pushRecommends(1:list<Recommend> recommends);
    void pushUserPages(1:list<UserPage> userPages);
}