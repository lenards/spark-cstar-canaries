package net.lenards.kinesis;

import java.io.Serializable;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class SerializableDefaultAWSCredentialsProviderChain
    extends DefaultAWSCredentialsProviderChain
    implements Serializable {

}