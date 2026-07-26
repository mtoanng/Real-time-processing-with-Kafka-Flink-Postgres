package com.taobao.behavior.model;

import com.taobao.behavior.avro.UserBehaviorEvent;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Durable core quality classification for invalid, duplicate, and late events. */
public class StreamQualityEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum QualityType {
        INVALID,
        DUPLICATE,
        LATE
    }

    private String qualityEventId;
    private String qualityType;
    private String eventId;
    private long userId;
    private long itemId;
    private long categoryId;
    private String behaviorType;
    private long eventTime;
    private String replayRunId;
    private long sourceSequence;
    private String reasonCode;
    private String reasonMessage;
    private long observedAt;

    public StreamQualityEvent() {}

    public StreamQualityEvent(
            String qualityEventId,
            String qualityType,
            String eventId,
            long userId,
            long itemId,
            long categoryId,
            String behaviorType,
            long eventTime,
            String replayRunId,
            long sourceSequence,
            String reasonCode,
            String reasonMessage,
            long observedAt) {
        this.qualityEventId = qualityEventId;
        this.qualityType = qualityType;
        this.eventId = eventId;
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaviorType = behaviorType;
        this.eventTime = eventTime;
        this.replayRunId = replayRunId;
        this.sourceSequence = sourceSequence;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.observedAt = observedAt;
    }

    public static StreamQualityEvent fromEvent(
            UserBehaviorEvent event,
            QualityType qualityType,
            String reasonCode,
            String reasonMessage,
            long observedAt) {
        String eventId =
                event == null || event.getEventId() == null
                        ? null
                        : event.getEventId().toString();
        String replayRunId =
                event == null || event.getReplayRunId() == null
                        ? ""
                        : event.getReplayRunId().toString();
        long sourceSequence = event == null ? -1L : event.getSourceSequence();
        String qualityEventId =
                deterministicQualityEventId(
                        qualityType, eventId, replayRunId, sourceSequence, reasonCode);
        return new StreamQualityEvent(
                qualityEventId,
                qualityType.name(),
                eventId,
                event == null ? 0L : event.getUserId(),
                event == null ? 0L : event.getItemId(),
                event == null ? 0L : event.getCategoryId(),
                event == null || event.getBehaviorType() == null
                        ? ""
                        : event.getBehaviorType().toString(),
                event == null ? 0L : event.getEventTimeMs(),
                replayRunId,
                sourceSequence,
                reasonCode,
                reasonMessage,
                observedAt);
    }

    static String deterministicQualityEventId(
            QualityType qualityType,
            String eventId,
            String replayRunId,
            long sourceSequence,
            String reasonCode) {
        String canonical =
                qualityType.name()
                        + "\u001f"
                        + (eventId == null ? "" : eventId)
                        + "\u001f"
                        + replayRunId
                        + "\u001f"
                        + sourceSequence
                        + "\u001f"
                        + reasonCode;
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exc) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", exc);
        }
    }

    public String getQualityEventId() {
        return qualityEventId;
    }

    public void setQualityEventId(String qualityEventId) {
        this.qualityEventId = qualityEventId;
    }

    public String getQualityType() {
        return qualityType;
    }

    public void setQualityType(String qualityType) {
        this.qualityType = qualityType;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehaviorType() {
        return behaviorType;
    }

    public void setBehaviorType(String behaviorType) {
        this.behaviorType = behaviorType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getReplayRunId() {
        return replayRunId;
    }

    public void setReplayRunId(String replayRunId) {
        this.replayRunId = replayRunId;
    }

    public long getSourceSequence() {
        return sourceSequence;
    }

    public void setSourceSequence(long sourceSequence) {
        this.sourceSequence = sourceSequence;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public void setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public void setReasonMessage(String reasonMessage) {
        this.reasonMessage = reasonMessage;
    }

    public long getObservedAt() {
        return observedAt;
    }

    public void setObservedAt(long observedAt) {
        this.observedAt = observedAt;
    }
}
