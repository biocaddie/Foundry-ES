package org.neuinfo.foundry.ws.common;

import org.neuinfo.foundry.common.model.ApiKeyInfo;
import org.neuinfo.foundry.common.model.User;
import org.neuinfo.foundry.common.util.Utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by bozyurt on 1/15/16.
 */
public class SecurityService {
    MongoService mongoService;
    Map<String, UserSession> userSessionMap = new ConcurrentHashMap<String, UserSession>();
    private static SecurityService instance = null;


    public synchronized static SecurityService getInstance(MongoService mongoService) {
        if (instance == null) {
            instance = new SecurityService(mongoService);
        }
        return instance;
    }

    public synchronized static SecurityService getInstance() {
        if (instance == null) {
            throw new RuntimeException("Not properly initialized!");
        }
        return instance;
    }


    private SecurityService(MongoService mongoService) {
        this.mongoService = mongoService;
        List<ApiKeyInfo> allApikeyInfos = this.mongoService.getAllApikeyInfos();
        for (ApiKeyInfo aki : allApikeyInfos) {
            if (aki.isPerpetual()) {
                UserSession session = new UserSession(aki, null, null);
                this.userSessionMap.put(aki.getApiKey(), session);
            }
        }
        Thread thread = new Thread(new CleanupWorker());
        thread.setDaemon(true);
        thread.start();
    }


    public String authenticate(String userName, String pwd) {
        User user = mongoService.findUser(userName, pwd);
        if (user == null) {
            return null;
        }
        ApiKeyInfo aki = mongoService.findApiKeyInfoForUser(user.getUsername());
        if (aki == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(80);
        Random rnd = new Random();
        sb.append(pwd).append(':').append(aki.getApiKey()).append(':').append(userName).append(':').append(rnd.nextLong());
        try {
            String sessionKey = Utils.getMD5ChecksumOfString(sb.toString());
            UserSession userSession = new UserSession(aki, sessionKey, user.getRole());
            this.userSessionMap.put(sessionKey, userSession);
            return sessionKey;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public void logout(String username) {
        for (Iterator<Map.Entry<String, UserSession>> iter = userSessionMap.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, UserSession> entry = iter.next();
            if (!entry.getValue().isPerpetual() && entry.getValue().apiKeyInfo.getUsername().equals(username)) {
                iter.remove();
                break;
            }
        }
    }

    public boolean isAuthenticated(String apiOrSessionKey) {
        return apiOrSessionKey != null && userSessionMap != null && userSessionMap.containsKey(apiOrSessionKey);
    }

    public boolean isAuthenticatedAndHasRole(String apiOrSessionKey, String role) {
        UserSession userSession = userSessionMap.get(apiOrSessionKey);
        if (userSession == null) {
            return false;
        }
        return role.equals(userSession.getRole());
    }


    public static class UserSession {
        final ApiKeyInfo apiKeyInfo;
        AtomicLong lastUsed;
        String sessionApiKey;
        String role;

        public UserSession(ApiKeyInfo apiKeyInfo, String sessionApiKey, String role) {
            this.apiKeyInfo = apiKeyInfo;
            if (!this.apiKeyInfo.isPerpetual()) {
                this.lastUsed = new AtomicLong(System.currentTimeMillis());
            }
            this.sessionApiKey = sessionApiKey;
            this.role = role;
        }

        public boolean isPerpetual() {
            return this.apiKeyInfo.isPerpetual();
        }

        public void setLastUsed(long timeMillis) {
            lastUsed.set(timeMillis);
        }

        public long getLastUsed() {
            return lastUsed.get();
        }

        public String getRole() {
            return role;
        }
    }

    class CleanupWorker implements Runnable {
        long checkInterval = 120000l; // 2 minutes
        long timeoutInterval = 1800000l; // 30 minutes

        @Override
        public void run() {
            while (true) {
                if (!userSessionMap.isEmpty()) {
                    long now = System.currentTimeMillis();
                    for (Iterator<Map.Entry<String, UserSession>> iter = userSessionMap.entrySet().iterator();
                         iter.hasNext(); ) {
                        Map.Entry<String, UserSession> entry = iter.next();
                        if (!entry.getValue().isPerpetual()) {
                            long diff = now - entry.getValue().getLastUsed();
                            if (diff >= timeoutInterval) {
                                iter.remove();
                            }
                        }
                    }
                }
                try {
                    synchronized (this) {
                        this.wait(checkInterval);
                    }
                } catch (InterruptedException e) {
                    // no op
                }
            }
        }
    }
}
