{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE ViewPatterns          #-}

-- | An HTTP Client to post data to a datadog agent.
--
-- Many of our refinements are stricter than the actual requirements, to err on
-- the side of caution, and because the actual requirements are very complex.
module Datadog.Client (
    Agent(..)
  , AgentT
  , DDText
  , HasAlpha
  , MetaKey(..)
  , MetaValue(..)
  , ServiceName(..)
  , Span(..)
  , SpanId(..)
  , SpanName(..)
  , Tag
  , Trace(..)
  , TraceId(..)
  , newServantAgent
  , newServantAgent'
) where

import           Control.Monad             (unless, void)
import           Control.Monad.Except      (ExceptT, MonadError, MonadIO,
                                            liftEither, liftIO, runExceptT)
import           Data.Char                 (isAlpha, isAsciiLower, isDigit)
import           Data.FFunctor             (FFunctor, ffmap)
import           Data.Int                  (Int64)
import qualified Data.List.NonEmpty        as NEL
import           Data.Map.Strict           (Map)
import qualified Data.Map.Strict           as M
import           Data.Text                 (Text)
import qualified Data.Text                 as T
import           Data.Text.Arbitrary       ()
import           Data.Text.Prettyprint.Doc (viaShow)
import           Data.Time                 (NominalDiffTime, UTCTime)
import           Data.Time.Clock.POSIX     (utcTimeToPOSIXSeconds)
import           Data.Typeable             (Proxy (..), Typeable, typeOf)
import           Data.Word                 (Word64)
import           Refined                   hiding (NonEmpty)
import qualified Refined
import           Servant.Client            (ClientEnv, ClientM, ClientError,
                                            client, runClientM)

import qualified Datadog.Agent             as API

-- | The Datadog Agent API, independent of any HTTP framework.
newtype Agent m = Agent
  { putTraces :: NEL.NonEmpty Trace -> m ()
  }

-- | Allows users to opt-out of having a MonadError in their Monad stack. They
--   opt-in to error handling at the points when calling the Agent. Requires
--   users to handle errors at the point of use.
--
--   See https://discourse.haskell.org/t/local-capabilities-with-mtl/231
type AgentT m = Agent (ExceptT ClientError m)

instance FFunctor Agent where
  ffmap nt (Agent p1) = Agent (nt . p1)

-- | An Agent (or AgentT) implemented by Servant.
newServantAgent :: (MonadIO m, MonadError ClientError m) => ClientEnv -> Agent m
newServantAgent env = ffmap (liftClientM env) (Agent traces)

-- | A fire-and-forget Agent implemented by Servant.
newServantAgent' :: MonadIO m => ClientEnv -> Agent m
newServantAgent' env =
  let agent = newServantAgent env
  in Agent (\t -> void . runExceptT $ putTraces agent t)

type DDText = Refined.NonEmpty && (SizeLessThan 101)

newtype SpanId = SpanId (Refined NonZero Word64) deriving (Eq, Show)
newtype TraceId = TraceId (Refined NonZero Word64) deriving (Eq, Show)
newtype ServiceName = ServiceName (Refined (DDText && Tag) Text) deriving (Eq, Show)

data Trace = Trace
  { tService :: ServiceName
  , tId      :: TraceId
  , tSpans   :: (Refined (Refined.NonEmpty) (Map SpanId Span))
  } deriving (Eq, Show)

newtype SpanName = SpanName (Refined (DDText && HasAlpha) Text) deriving (Eq, Show)
newtype MetaKey = MetaKey (Refined (DDText && Tag) Text) deriving (Eq, Ord, Show)
newtype MetaValue = MetaValue (Refined DDText Text) deriving (Eq, Show)

data Span = Span
  { sName     :: SpanName
  , sParentId :: Maybe SpanId
  , sStart    :: UTCTime
  , sDuration :: NominalDiffTime
  , sMeta     :: Maybe (Map MetaKey MetaValue)
  , sError    :: Bool
  } deriving (Eq, Show)

traces :: NEL.NonEmpty Trace -> ClientM ()
traces (NEL.toList -> ts) = void . raw $ toAPI <$> ts
  where
    raw = client (Proxy @ API.Traces3)

    toAPI :: Trace -> API.Trace
    toAPI (trace@(Trace _ _ (M.toList . unrefine -> spans))) =
      API.Trace $ (mkSpan trace) <$> spans

    mkSpan :: Trace -> (SpanId, Span) -> API.Span
    mkSpan (Trace (ServiceName (unrefine -> serviceName))
                  (TraceId (unrefine -> traceId))
                  _)
           ((SpanId (unrefine -> spanId)),
            (Span (SpanName (unrefine -> spanName))
             parent
             start
             duration
             meta
             err)) =
      -- Magic undocumented flags: never sample, always search
      let metrics = M.insert "_dd1.sr.eausr" 1 $ case parent of
            Just _  -> M.singleton "_sampling_priority_v1" 2
            Nothing -> M.empty
      in API.Span
               serviceName
               spanName
               spanName -- not using resource
               traceId
               spanId
               ((\(SpanId (unrefine -> p)) -> p) <$> parent)
               (timeToNanos start)
               (nominalToNanos duration)
               (if err then Just 1 else Nothing)
               ((\m -> (M.map unValue) . (M.mapKeys unKey) $ m) <$> meta)
               (Just metrics)
               Nothing -- not using type

    unKey (MetaKey (unrefine -> k)) = k
    unValue (MetaValue (unrefine -> v)) = v

    timeToNanos :: UTCTime -> Int64
    timeToNanos time = nominalToNanos $ utcTimeToPOSIXSeconds time

    nominalToNanos :: NominalDiffTime -> Int64
    nominalToNanos time =
      let (nanos, _) = properFraction (1000000000 * time)
      in  nanos

data Tag
instance Predicate Tag Text where
   validate p txt = validate' p (T.all isValidChar) txt
                    where
                      -- dumbed down `normalizeTag`
                      isValidChar c = case c of
                        ':' -> True
                        '.' -> True
                        '/' -> True
                        '-' -> True
                        c'  -> isAsciiLower c' || isDigit c'

data HasAlpha
instance Predicate HasAlpha Text where
   validate p txt = validate' p (T.any isAlpha) txt


validate' :: (Typeable t, Show a) => Proxy t -> (a -> Bool) -> a -> Maybe RefineException
-- validate' :: (Typeable t, Monad m, Show a) => t -> (a -> Bool) -> a -> RefineT m ()
validate' t p a =
  unless (p a) $
  throwRefineOtherException (typeOf t) ("failed predicate: " <> (viaShow a))

-- | Converts ClientM signatures into MTL, pushing errors into the stack.
liftClientM :: (MonadIO m, MonadError ClientError m)
  => ClientEnv
  -> ClientM a
  -> m a
liftClientM env ca = liftEither =<< (liftIO $ runClientM ca env)
