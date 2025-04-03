module signatures;

import std.stdio;
import std.file;
import std.string;
import std.digest.sha;
import std.algorithm;
import std.range;
import std.conv;

import deimos.openssl.rsa;
// import deimos.openssl.pkey;
import deimos.openssl.bio;
// import deimos.openssl.err;
// import deimos.openssl.evprsa;
import deimos.openssl.ssl;
import core.stdc.string;

import globals;

class SignatureAgent {
    private BIO* m_privateBio;
    private RSA* m_privateRsa;
    private BIO*[NodeId] m_publicBios;
    private RSA*[NodeId] m_publicRsas;
    this(NodeId myId, NodeId[] peers){
        loadPrivate(myId);
        foreach (peer; peers){
            loadPublic(peer);
        }
    }

    ~this(){
        RSA_free(m_privateRsa);
        BIO_free(m_privateBio);
        foreach(i; m_publicBios.keys){
            RSA_free(m_publicRsas[i]);
            BIO_free(m_publicBios[i]);
        }
    }

    bool loadPrivate(NodeId id){
        // --- Load Private Key for Signing ---
        char[] privateKeyPem = cast(char[]) readText(format("private_key_%d.pem", id));
        BIO* bioPriv = BIO_new_mem_buf(privateKeyPem.ptr, cast(int)privateKeyPem.length);
        RSA* rsaPriv = PEM_read_bio_RSAPrivateKey(bioPriv, null, null, null);
        if (rsaPriv is null) {
            writeln("Failed to read private key");
            return false;
        }
        m_privateBio = bioPriv;
        m_privateRsa = rsaPriv;
        return true;
    }

    bool loadPublic(NodeId id){
        // --- Load Public Key for Verification ---
        auto publicKeyPem = cast(char[]) readText(format("public_key_%d.pem", id));
        BIO* bioPub = BIO_new_mem_buf(publicKeyPem.ptr, cast(int)publicKeyPem.length);
        RSA* rsaPub = PEM_read_bio_RSA_PUBKEY(bioPub, null, null, null);
        if (rsaPub is null) {
            writeln("Failed to read public key");
            return false;
        }
        m_publicBios[id] = bioPub;
        m_publicRsas[id] = rsaPub;
        return true;
    }

    ubyte[SHA256_DIGEST_LENGTH] getMessageHashToSign(Message msg){
        ubyte[SHA256_DIGEST_LENGTH] hash;
        string toSign = msg.uniqueIdTrail.to!string;
        if (msg.type == Message.Type.RaftAppendEntries) {
            toSign ~= msg.content["subtype"].get!string;
            if (msg.content["subtype"].get!string == "clientRequest"){
                toSign ~= msg.content["content"].get!string;
            }
        }
        hash = sha256Of(toSign);
        return hash;
    }

    bool sign(ref Message msg){
        auto hash = getMessageHashToSign(msg);
        int signResult = RSA_sign(NID_sha256, hash.ptr, cast(uint)hash.length,
                                msg.signature.ptr, &msg.signatureLen, m_privateRsa);
        if (signResult != 1) {
            writeln("Failed to sign message");
            return false;
        }

        return true;
    }

    bool verify(Message msg, NodeId author){
        auto hash = getMessageHashToSign(msg);
        int verifyResult = RSA_verify(NID_sha256, hash.ptr, cast(uint)hash.length,
                                    msg.signature.ptr, msg.signatureLen, m_publicRsas[author]);

        if (verifyResult == 1) {
            return true;
        } else {
            return false;
        }
    }
}