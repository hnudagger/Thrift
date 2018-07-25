struct Label {
	1:string code;
	2:string attribute;
}
struct Interests {
	1:map<string,Label> labels;
}
struct Props {
	1:map<string,Label> labels;
}
struct Uimglabels {
    1:Props props;
	2:Interests interests;
}

service UimgService {
    Uimglabels getlabels(1:string uid);
    map<string,Uimglabels> getulabels(1:list<string> uids);
}