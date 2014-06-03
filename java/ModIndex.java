
// a = {2,3,1,0} 
// a = {1,0,3,2}

public static void modIndex(int[] array){ 
    int pre = array[0]; 
    int idx = 0; 
    array[0] = -array[pre]; 
    while (true){ 
        int i = 0; 
        for(; i < array.length;i++){ 
            if(array[i] == idx) 
                break; 
        } 
        if(i == array.length) break; 
        int t = array[i]; 
        array[i] = - pre; 
        pre = t; 
        idx = i; 

    } 

    for(int i = 0 ; i < array.length; i++) 
        array[i] *= -1; 
}
