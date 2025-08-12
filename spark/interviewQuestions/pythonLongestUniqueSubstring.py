seen = {}
s="saksham"
maxi = 0
start = 0 
string =""
best =(0,0)
for i in range(len(s)):
    if s[i] in seen.keys() and seen[s[i]]>=start:
         start= seen[s[i]]+1
    
    seen[s[i]] = i
    
    if i-start+1>best[1]-best[0]:
        best = (start,i+1)
        string = s[best[0]:best[1]]
print(string)
